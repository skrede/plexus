#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/object_carrier.h"
#include "plexus/inproc/detail/inproc_dispatch.h"
#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::inproc {

template<typename Clock>
class inproc_channel;

// over-limit: one cohesive in-process delivery bus; the register/listener/enqueue/deliver verbs +
// the grown-once packet ring all share the m_channels/m_listeners/m_ring state under the
// posted-only re-entrancy invariant, so splitting the surface scatters that shared bus state (the
// per-packet match/dispatch tail is extracted to detail/inproc_dispatch.h).
//
// In-process byte-packet delivery between registered channels: a channel registers on construction
// and is handed a synthetic endpoint + a never-reused integer key; send() enqueues a packet
// addressed by the peer's key, and deliver_one() hands exactly one queued packet to its target.
// Delivery is pull-driven by the executor's step-loop, never from the producing send() — the
// posted-only discipline that keeps the re-entrancy invariant structural.
template<typename Clock = std::chrono::steady_clock>
class inproc_bus
{
public:
    // A registered accepting endpoint and the callback a dial() to it fires with
    // the accepted channel end. Public so inproc_transport can hold the lookup.
    struct listener_entry
    {
        io::endpoint                                                             ep;
        detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> on_accepted;
    };

    inproc_bus() = default;

    inproc_bus(const inproc_bus &)            = delete;
    inproc_bus &operator=(const inproc_bus &) = delete;
    inproc_bus(inproc_bus &&)                 = delete;
    inproc_bus &operator=(inproc_bus &&)      = delete;

    io::endpoint register_channel(inproc_channel<Clock> *chan)
    {
        const std::uint64_t key = m_next_addr++;
        io::endpoint        assigned{"inproc", std::to_string(key)};
        m_channels.push_back(channel_entry{chan, assigned, key});
        return assigned;
    }

    // Resolve a bus-minted endpoint back to its channel key (0 = unregistered). Keys are never
    // reused, so a key resolved once at connect_to stays bound to that registration.
    [[nodiscard]] std::uint64_t key_for(const io::endpoint &ep) const noexcept
    {
        for(const auto &entry : m_channels)
            if(entry.assigned_ep == ep)
                return entry.key;
        return 0;
    }

    void deregister_channel(inproc_channel<Clock> *chan) noexcept
    {
        std::erase_if(m_channels, [chan](const channel_entry &e) { return e.chan == chan; });
    }

    // The accepting-endpoint registry (distinct from the synthetic per-channel addresses): a
    // listen() names an endpoint + the on_accepted callback a dial() to it fires.
    void register_listener(const io::endpoint &ep, detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> on_accepted)
    {
        m_listeners.push_back(listener_entry{ep, std::move(on_accepted)});
    }

    listener_entry *find_listener(const io::endpoint &ep) noexcept
    {
        for(auto &entry : m_listeners)
            if(entry.ep == ep)
                return &entry;
        return nullptr;
    }

    void deregister_listener(const io::endpoint &ep) noexcept
    {
        std::erase_if(m_listeners, [&ep](const listener_entry &e) { return e.ep == ep; });
    }

    void enqueue(std::uint64_t to_key, std::span<const std::byte> data, inproc_channel<Clock> *from)
    {
        queued_packet &slot = push_slot();
        slot.to_key         = to_key;
        slot.kind           = packet_kind::data;
        slot.from           = from;
        slot.data.assign(data.begin(), data.end()); // reuse the slot's grown capacity
        slot.carrier = {};
    }

    void enqueue_close(std::uint64_t to_key)
    {
        queued_packet &slot = push_slot();
        slot.to_key         = to_key;
        slot.kind           = packet_kind::close;
        slot.from           = nullptr;
        slot.data.clear();
        slot.carrier = {};
    }

    // Queue a process-tier object handle to a peer, sharing the byte FIFO so per-pair ordering
    // holds. The carrier rides inline (no payload copy). The bus addrefs; deliver_one releases it
    // after handing it on, and the not-found path releases it too so a vanished target never leaks.
    void enqueue_object(std::uint64_t to_key, const io::object_carrier &carrier)
    {
        io::addref(carrier);
        queued_packet &slot = push_slot();
        slot.to_key         = to_key;
        slot.kind           = packet_kind::object;
        slot.from           = nullptr;
        slot.data.clear();
        slot.carrier = carrier;
    }

    bool deliver_one()
    {
        if(m_size == 0)
            return false;
        queued_packet &pkt = m_ring[m_head];
        m_head             = (m_head + 1) % m_ring.size();
        --m_size;
        detail::dispatch_packet(*this, pkt); // match, deliver, and balance the bus reference
        return true;
    }

    [[nodiscard]] bool has_pending_packets() const noexcept
    {
        return m_size != 0;
    }

private:
    // The per-packet match + dispatch tail is relocated to detail/inproc_dispatch.h (a friend, so
    // it reaches the channel table + the packet_kind/sender_live members through the bus
    // reference).
    template<typename B, typename P>
    friend void detail::dispatch_packet(B &, P &);

    // Whether a queued packet's sender is still a registered channel (so its report_unroutable
    // target is live): a sender that deregistered between send and delivery is skipped.
    [[nodiscard]] bool sender_live(inproc_channel<Clock> *from) const noexcept
    {
        for(const auto &entry : m_channels)
            if(entry.chan == from)
                return true;
        return false;
    }

    enum class packet_kind : uint8_t
    {
        data,
        close,
        object
    };

    // Packets address their target by the registration key, never the endpoint value: per-enqueue
    // string assigns + a per-channel string compare were measurable on the publish loop. The
    // endpoint strings stay cold (minted at registration, compared only in key_for).
    struct queued_packet
    {
        std::uint64_t          to_key{0};
        packet_kind            kind{packet_kind::data};
        inproc_channel<Clock> *from{nullptr}; // the sender, for the unmatched-partner drop report
        std::vector<std::byte> data;
        io::object_carrier     carrier{};
    };

    struct channel_entry
    {
        inproc_channel<Clock> *chan;
        io::endpoint           assigned_ep;
        std::uint64_t          key;
    };

    // The FIFO rides a grown-once ring of reusable packet slots (not a deque, which churns block
    // allocations): the steady publish loop must be alloc-free. The ring doubles only on overflow
    // (never shrinks) and each slot reuses its data vector's capacity via assign — no steady heap.
    queued_packet &push_slot()
    {
        if(m_size == m_ring.size())
            grow();
        const std::size_t tail = (m_head + m_size) % m_ring.size();
        ++m_size;
        return m_ring[tail];
    }

    void grow()
    {
        const std::size_t          old_cap = m_ring.size();
        const std::size_t          new_cap = old_cap == 0 ? 8 : old_cap * 2;
        std::vector<queued_packet> next(new_cap);
        for(std::size_t i = 0; i < m_size; ++i)
            next[i] = std::move(m_ring[(m_head + i) % old_cap]);
        m_ring = std::move(next);
        m_head = 0;
    }

    std::vector<channel_entry>  m_channels;
    std::vector<listener_entry> m_listeners;
    std::vector<queued_packet>  m_ring;
    std::size_t                 m_head{0};
    std::size_t                 m_size{0};
    std::uint64_t               m_next_addr{1}; // key 0 stays "unconnected": it matches no registration
};

}

#endif

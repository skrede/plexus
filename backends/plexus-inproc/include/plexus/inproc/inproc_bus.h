#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/object_carrier.h"
#include "plexus/inproc/detail/inproc_dispatch.h"
#include "plexus/inproc/detail/inproc_packet_ring.h"
#include "plexus/inproc/detail/inproc_listener_set.h"
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

// In-process byte-packet delivery between registered channels: a channel registers on construction
// and is handed a synthetic endpoint + a never-reused integer key; send() enqueues a packet
// addressed by the peer's key, and deliver_one() hands exactly one queued packet to its target.
// Delivery is pull-driven by the executor's step-loop, never from the producing send() — the
// posted-only discipline that keeps the re-entrancy invariant structural.
template<typename Clock = std::chrono::steady_clock>
class inproc_bus
{
public:
    inproc_bus() = default;

    inproc_bus(const inproc_bus &)            = delete;
    inproc_bus &operator=(const inproc_bus &) = delete;
    inproc_bus(inproc_bus &&)                 = delete;
    inproc_bus &operator=(inproc_bus &&)      = delete;

    io::endpoint register_channel(inproc_channel<Clock> *chan)
    {
        const std::uint64_t key = m_next_addr++;
        io::endpoint assigned{"inproc", std::to_string(key)};
        m_channels.push_back(channel_entry{chan, assigned, key});
        return assigned;
    }

    // Keys are never reused, so a key resolved once at connect_to stays bound to that registration
    // (0 = unregistered).
    std::uint64_t key_for(const io::endpoint &ep) const noexcept
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

    std::uint64_t register_listener(const io::endpoint &ep, detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> on_accepted)
    {
        return m_listeners.add(ep, std::move(on_accepted));
    }

    template<typename Deliver>
    bool accept(const io::endpoint &ep, Deliver deliver)
    {
        return m_listeners.dispatch(ep, std::move(deliver));
    }

    void deregister_listener(std::uint64_t id) noexcept
    {
        m_listeners.remove(id);
    }

    void enqueue(std::uint64_t to_key, std::span<const std::byte> data, std::uint64_t from_key)
    {
        detail::inproc_packet &slot = m_ring.push_slot();
        slot.to_key                 = to_key;
        slot.kind                   = detail::inproc_packet_kind::data;
        slot.from_key               = from_key;
        // reuse the slot's grown capacity
        slot.data.assign(data.begin(), data.end());
        slot.carrier = {};
    }

    void enqueue_close(std::uint64_t to_key)
    {
        detail::inproc_packet &slot = m_ring.push_slot();
        slot.to_key                 = to_key;
        slot.kind                   = detail::inproc_packet_kind::close;
        slot.from_key               = 0;
        slot.data.clear();
        slot.carrier = {};
    }

    // Shares the byte FIFO so per-pair ordering holds. The bus addrefs; deliver_one (or the
    // not-found path) releases, so a vanished target never leaks.
    void enqueue_object(std::uint64_t to_key, const io::object_carrier &carrier)
    {
        io::addref(carrier);
        detail::inproc_packet &slot = m_ring.push_slot();
        slot.to_key                 = to_key;
        slot.kind                   = detail::inproc_packet_kind::object;
        slot.from_key               = 0;
        slot.data.clear();
        slot.carrier = carrier;
    }

    bool deliver_one()
    {
        if(m_ring.empty())
            return false;
        detail::dispatch_packet(*this, m_ring.pop_front());
        return true;
    }

    bool has_pending_packets() const noexcept
    {
        return !m_ring.empty();
    }

private:
    template<typename B, typename P>
    friend void detail::dispatch_packet(B &, P &);

    // The sender is addressed by its key like any target: one that deregistered since it sent is a miss.
    void report_unroutable(std::uint64_t from_key)
    {
        auto it = std::ranges::find(m_channels, from_key, &channel_entry::key);
        if(it != m_channels.end())
            it->chan->report_unroutable();
    }

    struct channel_entry
    {
        inproc_channel<Clock> *chan;
        io::endpoint assigned_ep;
        std::uint64_t key;
    };

    std::vector<channel_entry> m_channels;
    detail::inproc_listener_set<Clock> m_listeners;
    detail::inproc_packet_ring m_ring;
    std::uint64_t m_next_addr{1}; // key 0 stays unconnected, matching no registration
};

}

#endif

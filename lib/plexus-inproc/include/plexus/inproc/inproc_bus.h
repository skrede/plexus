#ifndef HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H
#define HPP_GUARD_PLEXUS_INPROC_INPROC_BUS_H

#include "plexus/io/endpoint.h"
#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::inproc {

template <typename Clock>
class inproc_channel;

// In-process byte-packet delivery between registered channels. A channel
// registers on construction and is handed a synthetic endpoint; send() enqueues
// a packet addressed to a peer endpoint, and deliver_one() hands exactly one
// queued packet to its target channel. Delivery is pull-driven by the executor's
// step-loop, never invoked from the producing send() — that posted-only discipline
// is what keeps the re-entrancy invariant structural.
template <typename Clock = std::chrono::steady_clock>
class inproc_bus
{
public:
    // A registered accepting endpoint and the callback a dial() to it fires with
    // the accepted channel end. Public so inproc_transport can hold the lookup.
    struct listener_entry
    {
        io::endpoint ep;
        detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> on_accepted;
    };

    inproc_bus() = default;

    inproc_bus(const inproc_bus &) = delete;
    inproc_bus &operator=(const inproc_bus &) = delete;
    inproc_bus(inproc_bus &&) = delete;
    inproc_bus &operator=(inproc_bus &&) = delete;

    io::endpoint register_channel(inproc_channel<Clock> *chan)
    {
        io::endpoint assigned{"inproc", std::to_string(m_next_addr++)};
        m_channels.push_back(channel_entry{chan, assigned});
        return assigned;
    }

    void deregister_channel(inproc_channel<Clock> *chan) noexcept
    {
        std::erase_if(m_channels, [chan](const channel_entry &e) { return e.chan == chan; });
    }

    // The accepting-endpoint registry (distinct from the synthetic per-channel
    // addresses above): a listen() names an endpoint and supplies the on_accepted
    // callback a dial() to that endpoint fires with the accepted channel end.
    void register_listener(const io::endpoint &ep,
                           detail::move_only_function<void(std::unique_ptr<inproc_channel<Clock>>)> on_accepted)
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

    void enqueue(const io::endpoint &to, std::span<const std::byte> data)
    {
        m_queue.push_back(queued_packet{to, packet_kind::data, std::vector<std::byte>(data.begin(), data.end())});
    }

    void enqueue_close(const io::endpoint &to)
    {
        m_queue.push_back(queued_packet{to, packet_kind::close, {}});
    }

    bool deliver_one()
    {
        if(m_queue.empty())
            return false;

        auto pkt = std::move(m_queue.front());
        m_queue.pop_front();

        for(auto &entry : m_channels)
            if(pkt.to == entry.assigned_ep)
            {
                if(pkt.kind == packet_kind::close)
                    entry.chan->deliver_close();
                else
                    entry.chan->deliver(std::span<const std::byte>(pkt.data));
                break;
            }

        return true;
    }

    [[nodiscard]] bool has_pending_packets() const noexcept { return !m_queue.empty(); }

private:
    enum class packet_kind : uint8_t { data, close };

    struct queued_packet
    {
        io::endpoint to;
        packet_kind kind;
        std::vector<std::byte> data;
    };

    struct channel_entry
    {
        inproc_channel<Clock> *chan;
        io::endpoint assigned_ep;
    };

    std::vector<channel_entry> m_channels;
    std::vector<listener_entry> m_listeners;
    std::deque<queued_packet> m_queue;
    uint32_t m_next_addr{1};
};

}

#endif

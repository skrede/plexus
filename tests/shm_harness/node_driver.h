#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_DRIVER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_DRIVER_H

#include "node_traffic.h"
#include "topology_block.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace shm_harness {

using publish_fn = plexus::detail::move_only_function<void(std::span<const std::byte>)>;

// An empty publish callback makes a node that only receives.
class node_driver
{
public:
    node_driver(node_slot &slot, std::uint32_t index, publish_fn publish_cb)
            : m_warming(false)
            , m_leaving(false)
            , m_seen(slot)
            , m_slot(slot)
            , m_index(index)
            , m_handled(slot.command_seq.load(std::memory_order_acquire))
            , m_next_warm(std::chrono::steady_clock::now())
            , m_publish_cb(std::move(publish_cb))
    {
    }

    void receive(std::span<const std::byte> bytes)
    {
        if(const std::optional<traffic_message> message = decode(bytes))
            m_seen.observe(*message);
    }

    bool serve()
    {
        const std::uint32_t seq = m_slot.command_seq.load(std::memory_order_acquire);
        if(seq != m_handled)
        {
            m_handled = seq;
            execute(static_cast<node_command>(m_slot.command.load(std::memory_order_acquire)), m_slot.arg.load(std::memory_order_acquire));
            m_slot.ack_seq.store(seq, std::memory_order_release);
        }
        if(m_warming && m_publish_cb && std::chrono::steady_clock::now() >= m_next_warm)
        {
            publish_one(true);
            m_next_warm = std::chrono::steady_clock::now() + std::chrono::milliseconds(10);
        }
        return !m_leaving;
    }

private:
    bool m_warming;
    bool m_leaving;
    seen_set m_seen;
    node_slot &m_slot;
    std::uint32_t m_index;
    std::uint32_t m_handled;
    std::chrono::steady_clock::time_point m_next_warm;
    publish_fn m_publish_cb;

    void execute(node_command command, std::uint32_t arg)
    {
        m_warming = command == node_command::warm;
        m_leaving = command == node_command::leave;
        if(command == node_command::reset)
            m_seen.reset();
        for(std::uint32_t i = 0; command == node_command::publish && m_publish_cb && i < arg; ++i)
            publish_one(false);
    }

    void publish_one(bool warm)
    {
        const std::uint32_t sequence         = warm ? 0 : m_slot.published.load(std::memory_order_relaxed);
        const std::array<std::byte, 8> bytes = encode({static_cast<std::uint16_t>(m_index), sequence, warm});
        m_publish_cb(bytes);
        if(!warm)
            m_slot.published.fetch_add(1, std::memory_order_release);
    }
};

}

#endif

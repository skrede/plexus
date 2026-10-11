#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_TRAFFIC_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_TRAFFIC_H

#include "topology_block.h"

#include <span>
#include <array>
#include <atomic>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace shm_harness {

inline constexpr std::uint32_t k_max_sequence = 1024;
inline constexpr std::byte k_traffic_magic{0xA5};

struct traffic_message
{
    std::uint16_t publisher;
    std::uint32_t sequence;
    bool warm;
};

inline std::array<std::byte, 8> encode(const traffic_message &m)
{
    const auto at = [](std::uint32_t v, std::uint32_t shift) { return std::byte((v >> shift) & 0xFFu); };
    return {at(m.publisher, 0), at(m.publisher, 8), at(m.sequence, 0), at(m.sequence, 8), at(m.sequence, 16), at(m.sequence, 24), std::byte(m.warm ? 1 : 0), k_traffic_magic};
}

inline std::optional<traffic_message> decode(std::span<const std::byte> b)
{
    if(b.size() != 8 || b[7] != k_traffic_magic)
        return std::nullopt;
    const auto u         = [&](std::size_t i) { return std::to_integer<std::uint32_t>(b[i]); };
    const auto publisher = static_cast<std::uint16_t>(u(0) | (u(1) << 8));
    return traffic_message{publisher, u(2) | (u(3) << 8) | (u(4) << 16) | (u(5) << 24), u(6) != 0};
}

class seen_set
{
public:
    explicit seen_set(node_slot &slot)
            : m_slot(slot)
            , m_seen()
    {
    }

    void observe(const traffic_message &m)
    {
        if(m.warm)
        {
            if(m.publisher < k_max_nodes)
                m_slot.warm_seen.fetch_or(1u << m.publisher, std::memory_order_release);
            return;
        }
        m_slot.received.fetch_add(1, std::memory_order_release);
        if(m.publisher >= k_max_nodes || m.sequence >= k_max_sequence)
            return;
        auto &seen = m_seen[m.publisher];
        if(seen.test(m.sequence))
            m_slot.duplicates.fetch_add(1, std::memory_order_release);
        else
            m_slot.distinct.fetch_add(1, std::memory_order_release);
        seen.set(m.sequence);
    }

    void reset()
    {
        for(auto &seen : m_seen)
            seen.reset();
        m_slot.received.store(0, std::memory_order_release);
        m_slot.distinct.store(0, std::memory_order_release);
        m_slot.duplicates.store(0, std::memory_order_release);
    }

private:
    node_slot &m_slot;
    std::array<std::bitset<k_max_sequence>, k_max_nodes> m_seen;
};

}

#endif

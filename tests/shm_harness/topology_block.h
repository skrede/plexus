#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_BLOCK_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_BLOCK_H

#include "plexus/io/upgrade_channel.h"

#include <array>
#include <atomic>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace shm_harness {

inline constexpr std::uint32_t k_max_nodes = 8;

enum class node_role : std::uint32_t
{
    pub,
    sub,
    pubsub,
};

inline std::string_view role_name(node_role role)
{
    switch(role)
    {
        case node_role::pub:
            return "pub";
        case node_role::sub:
            return "sub";
        case node_role::pubsub:
            return "pubsub";
    }
    return "";
}

// same_host builds the node through the production same-host composition; arm_denied swaps in a shared-memory member
// whose every notifier fails to arm; runtime_lost one whose every notifier arms and then loses its waiter; single_reader
// is same_host with the publisher's ring provisioned for one consumer.
enum class node_composition : std::uint32_t
{
    same_host,
    arm_denied,
    runtime_lost,
    single_reader,
};

inline std::string_view composition_name(node_composition composition)
{
    switch(composition)
    {
        case node_composition::arm_denied:
            return "arm_denied";
        case node_composition::runtime_lost:
            return "runtime_lost";
        case node_composition::single_reader:
            return "single_reader";
        case node_composition::same_host:
            break;
    }
    return "same_host";
}

enum class dial_wiring : std::uint32_t
{
    one_way,
    crossed,
};

enum class node_command : std::uint32_t
{
    none,
    warm,
    publish,
    reset,
    leave,
};

struct node_slot
{
    std::atomic<std::uint32_t> ready;
    std::atomic<std::uint32_t> connected;
    std::atomic<std::uint32_t> command;
    std::atomic<std::uint32_t> command_seq;
    std::atomic<std::uint32_t> ack_seq;
    std::atomic<std::uint32_t> arg;
    std::atomic<std::uint32_t> published;
    std::atomic<std::uint32_t> received;
    std::atomic<std::uint32_t> distinct;
    std::atomic<std::uint32_t> duplicates;
    std::atomic<std::uint32_t> out_of_order;
    std::atomic<std::uint32_t> warm_seen;
    std::atomic<std::uint32_t> slowest_send_us;
    std::atomic<std::uint32_t> exit_code;
    std::array<std::atomic<std::uint32_t>, plexus::io::k_fallback_reason_count> fallbacks;
};

struct topology_block
{
    std::array<node_slot, k_max_nodes> nodes;
};

struct node_counts
{
    std::uint32_t connected;
    std::uint32_t published;
    std::uint32_t received;
    std::uint32_t distinct;
    std::uint32_t duplicates;
    std::uint32_t out_of_order;
    std::uint32_t warm_seen;
    std::uint32_t slowest_send_us;
    plexus::io::fallback_counts fallbacks;
};

inline plexus::io::fallback_counts snapshot_fallbacks(const node_slot &s)
{
    plexus::io::fallback_counts out{};
    for(std::size_t reason = 0; reason < out.size(); ++reason)
        out[reason] = s.fallbacks[reason].load(std::memory_order_acquire);
    return out;
}

inline node_counts snapshot(const node_slot &s)
{
    return {s.connected.load(std::memory_order_acquire), s.published.load(std::memory_order_acquire),       s.received.load(std::memory_order_acquire),
            s.distinct.load(std::memory_order_acquire),  s.duplicates.load(std::memory_order_acquire),      s.out_of_order.load(std::memory_order_acquire),
            s.warm_seen.load(std::memory_order_acquire), s.slowest_send_us.load(std::memory_order_acquire), snapshot_fallbacks(s)};
}

inline std::uint64_t fallbacks_of(const node_counts &c, plexus::io::fallback_reason reason)
{
    return c.fallbacks[static_cast<std::size_t>(reason)];
}

// Each struck fallback reason as index:count.
inline std::string fallback_summary(const node_counts &c)
{
    std::string out;
    for(std::size_t reason = 0; reason < c.fallbacks.size(); ++reason)
        if(c.fallbacks[reason] != 0)
            out += (out.empty() ? "" : ",") + std::to_string(reason) + ":" + std::to_string(c.fallbacks[reason]);
    return out.empty() ? "none" : out;
}

inline std::string summary(const node_counts &c, bool over_shm)
{
    return "received=" + std::to_string(c.received) + " distinct=" + std::to_string(c.distinct) + " duplicates=" + std::to_string(c.duplicates) +
            " out_of_order=" + std::to_string(c.out_of_order) + " warm_seen=" + std::to_string(c.warm_seen) + " connected=" + std::to_string(c.connected) +
            " shm_region=" + (over_shm ? "1" : "0") + " fallbacks=" + fallback_summary(c);
}

}

#endif

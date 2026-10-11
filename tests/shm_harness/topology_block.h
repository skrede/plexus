#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_BLOCK_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_TOPOLOGY_BLOCK_H

#include <array>
#include <atomic>
#include <string>
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
// whose every notifier fails to arm; runtime_lost one whose every notifier arms and then loses its waiter.
enum class node_composition : std::uint32_t
{
    same_host,
    arm_denied,
    runtime_lost,
};

inline std::string_view composition_name(node_composition composition)
{
    if(composition == node_composition::runtime_lost)
        return "runtime_lost";
    return composition == node_composition::arm_denied ? "arm_denied" : "same_host";
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
    std::atomic<std::uint32_t> warm_seen;
    std::atomic<std::uint32_t> slowest_send_us;
    std::atomic<std::uint32_t> exit_code;
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
    std::uint32_t warm_seen;
    std::uint32_t slowest_send_us;
};

inline node_counts snapshot(const node_slot &s)
{
    return {s.connected.load(std::memory_order_acquire),      s.published.load(std::memory_order_acquire),  s.received.load(std::memory_order_acquire),
            s.distinct.load(std::memory_order_acquire),       s.duplicates.load(std::memory_order_acquire), s.warm_seen.load(std::memory_order_acquire),
            s.slowest_send_us.load(std::memory_order_acquire)};
}

inline std::string summary(const node_counts &c, bool over_shm)
{
    return "received=" + std::to_string(c.received) + " distinct=" + std::to_string(c.distinct) + " duplicates=" + std::to_string(c.duplicates) +
            " warm_seen=" + std::to_string(c.warm_seen) + " connected=" + std::to_string(c.connected) + " shm_region=" + (over_shm ? "1" : "0");
}

}

#endif

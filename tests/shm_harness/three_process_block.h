#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_THREE_PROCESS_BLOCK_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_THREE_PROCESS_BLOCK_H

#include <array>
#include <atomic>
#include <cstdint>
#include <string_view>

namespace shm_harness {

inline constexpr std::uint32_t k_three_roles           = 3;
inline constexpr std::uint32_t k_three_publisher       = 0;
inline constexpr std::string_view k_platform_notifier  = "platform";
inline constexpr std::string_view k_posix_sem_notifier = "posix_sem";
inline constexpr std::uint32_t k_three_command_publish = 1;
inline constexpr std::uint32_t k_three_command_leave   = 2;

// The parent stores a command, then bumps command_seq; the role acknowledges by copying command_seq to ack_seq.
struct three_role_slot
{
    std::atomic<std::uint32_t> armed;
    std::atomic<std::uint32_t> delivered;
    std::atomic<std::uint32_t> command;
    std::atomic<std::uint32_t> command_seq;
    std::atomic<std::uint32_t> ack_seq;
    std::atomic<std::uint32_t> last_latency_us;
};

struct three_process_block
{
    std::array<three_role_slot, k_three_roles> roles;
};

inline void issue_command(three_role_slot &slot, std::uint32_t command)
{
    slot.command.store(command, std::memory_order_release);
    slot.command_seq.fetch_add(1, std::memory_order_acq_rel);
}

inline bool acknowledged(const three_role_slot &slot)
{
    return slot.ack_seq.load(std::memory_order_acquire) == slot.command_seq.load(std::memory_order_acquire);
}

}

#endif

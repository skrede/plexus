#include "child_args.h"
#include "three_process_block.h"

#if !defined(_WIN32)
    #include "sem_support.h"
#endif

#include "support/region_names.h"
#include "support/xproc_control.h"
#include "support/xproc_harness.h"

#include "plexus/asio/shm/shm_member.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/wire_bytes.h"

#include <asio/io_context.hpp>

#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>

#if defined(__linux__)
    #include <csignal>

    #include <sys/prctl.h>
#endif

namespace {

using namespace shm_harness;
using namespace std::chrono_literals;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

struct platform_leg
{
    using registry = plexus::asio::shm::shm_member::registry_type;

    static registry::notifier_binder binder(::asio::io_context &io)
    {
        return plexus::asio::shm::make_bridge_binder(io);
    }
};

std::int64_t now_ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// The steady clock is system-wide on every platform, so a stamp taken in the publisher process times the delivery.
void record_delivery(three_role_slot &slot, const plexus::wire_bytes<plexus::shm::shm_slot_owner> &message)
{
    std::int64_t stamp = 0;
    if(message.size() == sizeof(stamp))
    {
        std::memcpy(&stamp, message.data(), sizeof(stamp));
        slot.last_latency_us.store(static_cast<std::uint32_t>((now_ns() - stamp) / 1000), std::memory_order_release);
    }
    slot.delivered.fetch_add(1, std::memory_order_acq_rel);
}

template<typename Registry>
bool publish_stamp(Registry &registry, const std::string &fqn)
{
    const std::int64_t stamp = now_ns();
    std::array<std::byte, sizeof(stamp)> payload{};
    std::memcpy(payload.data(), &stamp, sizeof(stamp));
    auto *channel = registry.channel_for(fqn, k_request, plexus::testing::test_publisher());
    return channel != nullptr && channel->send(payload) == plexus::shm::loan_status::ok;
}

bool report_failure(const plexus::shm::acquire_failure &failure)
{
    std::cout << "three_process role acquire failed: bound=" << static_cast<std::uint32_t>(failure.bound) << " arm=" << static_cast<std::uint32_t>(failure.arm)
              << " broker=" << static_cast<std::uint32_t>(failure.broker) << " ask_bytes=" << failure.ask_bytes << " limit_bytes=" << failure.limit_bytes << std::endl;
    return false;
}

template<typename Registry>
bool serve_commands(Registry &registry, ::asio::io_context &io, const std::string &fqn, three_role_slot &slot)
{
    std::uint32_t acked = 0;
    while(true)
    {
        io.restart();
        io.run_for(1ms);
        const std::uint32_t seq = slot.command_seq.load(std::memory_order_acquire);
        if(seq == acked)
            continue;
        const std::uint32_t command = slot.command.load(std::memory_order_acquire);
        if(command == k_three_command_publish && !publish_stamp(registry, fqn))
            return false;
        acked = seq;
        slot.ack_seq.store(seq, std::memory_order_release);
        if(command == k_three_command_leave)
            return true;
    }
}

template<typename Leg>
bool run_role(const std::string &fqn, three_role_slot &slot, bool publisher)
{
    ::asio::io_context io;
    typename Leg::registry::broker_type broker;
    typename Leg::registry registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block, Leg::binder(io)};
    if(registry.acquire(fqn, k_request, plexus::testing::test_publisher(), 0, plexus::io::ring_geometry_mode::reliable_preserving, 0, plexus::shm::acquire_mode::join_live) ==
       plexus::shm::acquire_result::failed)
        return report_failure(registry.last_acquire_failure());

    if(!publisher)
        registry.set_consumer_sink(fqn, k_request, plexus::testing::test_publisher(),
                                   [&slot](plexus::wire_bytes<plexus::shm::shm_slot_owner> message) { record_delivery(slot, message); });
    slot.armed.store(1, std::memory_order_release);
    const bool served = serve_commands(registry, io, fqn, slot);
    registry.release(fqn, k_request, plexus::testing::test_publisher());
    return served;
}

bool run_three_role(const std::vector<std::string> &argv, bool publisher)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    const std::optional<std::uint32_t> index = argv.size() == 4 ? to_u32(argv[1]) : std::nullopt;
    auto block                               = plexus::testing::xproc_shared<three_process_block>::attach(argv.empty() ? std::string() : argv[0]);
    if(!index || *index >= k_three_roles || (*index == k_three_publisher) != publisher || !block)
    {
        std::cout << "three_process role: invalid arguments or the shared block did not attach" << std::endl;
        return false;
    }

    three_role_slot &slot = block->get().roles[*index];
    if(argv[3] == k_platform_notifier)
        return run_role<platform_leg>(argv[2], slot, publisher);
#if !defined(_WIN32)
    if(argv[3] == k_posix_sem_notifier)
        return run_role<posix_sem_leg>(argv[2], slot, publisher);
#endif
    std::cout << "three_process role: unknown notifier " << argv[3] << std::endl;
    return false;
}

bool three_pub(const std::vector<std::string> &argv)
{
    return run_three_role(argv, true);
}

bool three_sub(const std::vector<std::string> &argv)
{
    return run_three_role(argv, false);
}

[[maybe_unused]] const bool s_pub_registered = plexus::testing::register_xproc_child("shm_harness.three_pub", &three_pub);
[[maybe_unused]] const bool s_sub_registered = plexus::testing::register_xproc_child("shm_harness.three_sub", &three_sub);

}

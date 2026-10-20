#include "child_args.h"

#include "support/region_names.h"
#include "support/noop_notifier.h"
#include "support/xproc_harness.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"
#include "plexus/shm/shm_topic_registry.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <span>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <optional>

#if defined(__linux__)
    #include <csignal>

    #include <sys/prctl.h>
#endif

namespace {

// Attaches the region the parent created and holds the slot it names until the parent kills it; the sleep is bounded
// so a child whose parent died ends on its own where no parent-death signal exists.
bool slot_holder_child(const std::vector<std::string> &argv)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    const std::optional<std::uint32_t> slot = argv.size() == 2 ? shm_harness::to_u32(argv[1]) : std::nullopt;
    if(!slot)
        return false;
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle region;
    if(broker.attach(argv[0], region) != plexus::shm::region_status::ok || region.hold_slot(*slot) != plexus::shm::slot_hold::held)
        return false;
    std::this_thread::sleep_for(std::chrono::seconds(60));
    return true;
}

[[maybe_unused]] const bool s_slot_holder_registered = plexus::testing::register_xproc_child("shm_harness.slot_holder", &slot_holder_child);

using creator_registry = plexus::shm::shm_topic_registry<plexus::native::shm_region_broker, plexus::testing::noop_notifier>;

constexpr plexus::shm::ring_direction k_request = plexus::shm::ring_direction::request;

// The ring as the creator's own process sees it through a second mapping, which never removes a name.
struct ring_peek
{
    ring_peek()
            : broker()
            , control()
            , slab()
            , ring()
    {
    }

    bool map(const std::string &fqn)
    {
        const std::string name = plexus::testing::region_name(fqn, plexus::testing::test_publisher());
        return broker.attach(name, control) == plexus::shm::region_status::ok && broker.attach(name + ".s", slab) == plexus::shm::region_status::ok &&
                plexus::shm::broadcast_ring::attach(control.bytes(), slab.bytes(), plexus::testing::test_publisher(), ring) == plexus::shm::loan_status::ok;
    }

    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle control;
    plexus::native::shm_region_handle slab;
    plexus::shm::broadcast_ring ring;
};

struct creator_args
{
    std::string fqn;
    std::string mode;
    std::uint32_t first;
    std::uint32_t count;
};

std::optional<creator_args> parse_creator(const std::vector<std::string> &argv)
{
    const std::optional<std::uint32_t> first = argv.size() == 4 ? shm_harness::to_u32(argv[2]) : std::nullopt;
    const std::optional<std::uint32_t> count = argv.size() == 4 ? shm_harness::to_u32(argv[3]) : std::nullopt;
    if(!first || !count)
        return std::nullopt;
    return creator_args{argv[0], argv[1], *first, *count};
}

// Nothing is published before the parent's reader holds its cursor, so the reader sees every message.
bool publish_to_reader(creator_registry &registry, ring_peek &peek, const creator_args &args)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while(peek.ring.registered_high_water() == 0 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    auto *channel = registry.channel_for(args.fqn, k_request, plexus::testing::test_publisher());
    for(std::uint32_t value = args.first; channel != nullptr && value < args.first + args.count; ++value)
        if(channel->send(std::as_bytes(std::span<const std::uint32_t, 1>(&value, 1))) != plexus::shm::loan_status::ok)
            return false;
    return channel != nullptr;
}

// clean publishes and leaves; claim publishes, then wins one position it never commits and waits to be killed; hold
// keeps the creator slot until it is killed.
bool run_creator(creator_registry &registry, ring_peek &peek, const creator_args &args)
{
    if(args.mode != "hold" && !publish_to_reader(registry, peek, args))
        return false;
    if(args.mode == "clean")
    {
        registry.release(args.fqn, k_request, plexus::testing::test_publisher(), plexus::shm::ring_role::producer);
        return true;
    }
    plexus::shm::broadcast_ring::claim_result claimed;
    if(args.mode == "claim" &&
       peek.ring.claim_with_policy(sizeof(std::uint32_t), plexus::io::reliability::reliable, plexus::io::congestion::block, claimed) != plexus::shm::loan_status::ok)
        return false;
    std::this_thread::sleep_for(std::chrono::seconds(60));
    return true;
}

// refused succeeds only when its acquire is refused as creator_alive.
bool ring_creator_child(const std::vector<std::string> &argv)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    const std::optional<creator_args> args = parse_creator(argv);
    if(!args)
        return false;
    plexus::native::shm_region_broker broker;
    creator_registry registry{broker, plexus::io::reliability::reliable, plexus::io::congestion::block};
    const bool acquired = registry.acquire(args->fqn, k_request, plexus::testing::test_publisher(), plexus::shm::ring_role::producer, 64) != plexus::shm::acquire_result::failed;
    if(args->mode == "refused")
        return !acquired && registry.last_acquire_failure().bound == plexus::shm::acquire_bound::creator_alive;
    ring_peek peek;
    return acquired && peek.map(args->fqn) && run_creator(registry, peek, *args);
}

[[maybe_unused]] const bool s_ring_creator_registered = plexus::testing::register_xproc_child("shm_harness.ring_creator", &ring_creator_child);

}

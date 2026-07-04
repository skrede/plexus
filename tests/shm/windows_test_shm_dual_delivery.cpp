#include "plexus/asio/shm/windows/event_notifier.h"

#include "plexus/asio/detail/same_host_shm_config.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/taken_message.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/detail/compat.h"

#include "plexus/testing/platform.h"

#include "support/xproc_child_main.h"

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <optional>
#include <type_traits>

// The Windows cross-process wake + delivery proof on the re-exec (open-by-name) harness: the
// PARENT creates the named control+slab regions, builds the broadcast ring, and registers TWO
// slot_subscriber cursors at the tail BEFORE any publish, so both see the message the child
// later commits; it arms ONE event_notifier on the ring's notify word + the named auto-reset
// doorbell whose name derives from the region name. The CHILD is a fresh process image that
// re-opens the SAME regions BY NAME (the names arrive by argv), attaches the ring, publishes ONE
// known value, and rings the doorbell (bump the notify word + SetEvent). run_xproc runs the child
// to exit FIRST, then the parent predicate pumps the io_context: the latched auto-reset wake
// completes the notifier's async_wait, the drain posts, and BOTH subscribers take the exact value.
// An auto-reset event wakes one waiter per signal, so a SINGLE notifier's drain services BOTH
// cursors -- the two-subscriber shape is what would expose a single-wake / wrong-cursor defect.
// Looped in-body; the ctest binary is re-run for reproducibility.

namespace pio = plexus::shm;
namespace pas = plexus::asio::shm;
using plexus::native::shm_region_broker;
using plexus::native::shm_region_handle;

// Composition-level shm-path witness: the portable on-host substrate resolves to the shm fast
// path on Windows, and its shm member composes over the very broker this test drives.
static_assert(PLEXUS_SAME_HOST_SHM == 1, "the Windows on-host composition must select the shm fast path");
static_assert(std::is_same_v<pas::shm_member::registry_type::broker_type, shm_region_broker>,
              "the on-host shm member must compose over the Windows shm region broker this test drives");

namespace {

struct test_policy
{
    using executor_type = ::asio::io_context &;
    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

constexpr std::uint32_t k_payload = 0x5B0AD11Eu;

std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}
std::string slab_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request) + ".s";
}

// The producer child, dispatched in the re-exec'd process by key: attach the parent's regions BY
// NAME, publish ONE value over the ring, and ring the named doorbell so the parent's notifier
// wakes. The signal LATCHES on the auto-reset event because the parent is not yet pumping.
bool two_subscriber_producer(const std::vector<std::string> &args)
{
    if(args.size() < 2)
        return false;

    shm_region_broker broker;
    shm_region_handle ctrl, slab;
    if(broker.attach(args[0], ctrl) != pio::region_status::ok || broker.attach(args[1], slab) != pio::region_status::ok)
        return false;

    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable, plexus::io::congestion::block, claim) != pio::loan_status::ok)
        return false;
    std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
    if(ring.commit(claim.position, sizeof(k_payload)) != pio::loan_status::ok)
        return false;

    // Open the SAME named doorbell the parent armed (arm derives the event name from the region
    // name) and ring it. The child never runs its own reactor; the wait is only ever the parent's.
    ::asio::io_context child_io;
    pas::event_notifier<test_policy> doorbell(child_io, ring.notify_generation(), ring.park_state(), args[0]);
    doorbell.arm([] {});
    doorbell.signal();
    return true;
}

[[maybe_unused]] const bool s_two_subscriber_registered = plexus::testing::register_xproc_child("shm.xproc_two_subscriber.producer", &two_subscriber_producer);

}

TEST_CASE("shm.xproc_two_subscriber a windows cross-process shm wake delivers one value to both subscribers", "[shm][xproc_two_subscriber]")
{
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 3; ++iter)
    {
        // A per-iteration name so a region freed on last-handle-close never races a same-name
        // create on the next turn.
        const std::string fqn       = "topic.two_subscriber." + std::to_string(plexus::testing::process_id()) + "." + std::to_string(iter);
        const std::string ctrl_name = control_name(fqn);
        const std::string slab_nm   = slab_name(fqn);

        // PARENT: create the regions + ring and register BOTH cursors at the tail BEFORE any
        // publish, then arm ONE doorbell whose drain services both. Hold the region handles alive
        // across the child (Windows frees a named mapping on last-handle-close).
        shm_region_broker broker;
        shm_region_handle ctrl, slab;
        REQUIRE(broker.create(ctrl_name, pio::control_region_bytes(geom.cell_count), pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_nm, pio::slab_region_bytes(geom.cell_count, geom.slot_capacity), pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count, geom.slot_capacity, ring) == pio::loan_status::ok);

        pio::slot_subscriber sub_a(ring);
        pio::slot_subscriber sub_b(ring);
        REQUIRE(sub_a.registered());
        REQUIRE(sub_b.registered());

        ::asio::io_context io;
        pas::event_notifier<test_policy> notifier(io, ring.notify_generation(), ring.park_state(), ctrl_name);

        std::uint32_t got_a = 0;
        std::uint32_t got_b = 0;
        notifier.arm(
                [&]
                {
                    pio::taken_message m;
                    while(sub_a.take(m) == pio::loan_status::ok)
                        if(m.bytes().size() == sizeof(std::uint32_t))
                            std::memcpy(&got_a, m.bytes().data(), sizeof(got_a));
                    while(sub_b.take(m) == pio::loan_status::ok)
                        if(m.bytes().size() == sizeof(std::uint32_t))
                            std::memcpy(&got_b, m.bytes().data(), sizeof(got_b));
                });

        const auto outcome = plexus::testing::run_xproc(
                "shm.xproc_two_subscriber.producer",
                {ctrl_name, slab_nm},
                [&]() -> bool
                {
                    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
                    while((got_a != k_payload || got_b != k_payload) && std::chrono::steady_clock::now() < deadline)
                        io.run_for(std::chrono::milliseconds(20));
                    notifier.disarm();
                    return got_a == k_payload && got_b == k_payload;
                });

        REQUIRE(outcome.child_succeeded);  // the child attached by name and published + signaled
        REQUIRE(outcome.parent_succeeded); // both subscribers took the value across the process boundary
    }
}

int main(int argc, char **argv)
{
    plexus::testing::xproc_capture_argv(argc, argv);
    plexus::testing::xproc_child_main(argc, argv); // _Exits when re-exec'd as the child role
    return Catch::Session().run(argc, argv);
}

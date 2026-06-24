// over-limit: one cohesive wakeup->reactor bridge matrix; the cells share the one forked
// producer + io_uring-futex reactor-bridge harness over a converged region, and that shared
// fixture preamble plus its produce helpers cannot split across TUs without scattering that
// shared cross-process bridge state into near-empty per-cell shells.
#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"
#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/shm_slot_owner.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <utility>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The wakeup -> reactor bridge functional proof (xproc): a PRODUCER process
// publishes into the shared ring and signals the in-region notify word; the
// CONSUMER (parent) has armed the io_uring-futex bridge on its OWN asio reactor.
// The parent drives ONLY its user-owned io_context (run_for) -- there is no
// plexus-spawned loop. The cross-process futex wake reaps the io_uring CQE on a
// reactor turn, POSTS the arm()'d drain onto the io_context, and the drain hands
// the published value up through the shm_channel (the registry's drain primitive).
// Looped N>=100 in-body; the ctest binary is re-run >=3 times for reproducibility.

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

namespace {

// A minimal Policy bundle for the bridge: it needs ONLY the executor_type (the
// user-owned io_context reference) + the static post() forwarding onto it. The
// bridge posts the drain via Policy::post -- the on_data-posted model, no
// serialized executor wrapper.
struct test_policy
{
    using executor_type = ::asio::io_context &;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

struct coord
{
    std::atomic<std::uint32_t> regions_ready{0}; // consumer: regions mapped + ring stamped + armed
    std::atomic<std::uint32_t> published{0};     // producer: value committed + signaled
    std::atomic<std::uint32_t> value_seen{0};    // consumer: drain delivered the value
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1,
                     0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}
std::string slab_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request) + ".s";
}

constexpr std::uint32_t k_payload = 0xBEEF1234u;

// The producer child: attach the consumer's regions, publish ONE value into the
// ring, then signal the in-region notify word (the cross-process futex wake the
// parent's bridge is parked on).
bool produce(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle           ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok ||
       broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;

    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable,
                              plexus::io::congestion::block, claim) != pio::loan_status::ok)
        return false;
    std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
    if(ring.commit(claim.position, sizeof(k_payload)) != pio::loan_status::ok)
        return false;

    plexus::native::notifier_signal(ring.notify_generation());
    return true;
}

// The GATED producer child: attach, publish ONE value, then drive the TWO-ARG gated
// signal over (generation, park_state). The wake is issued only if the consumer's
// notifier stored PARKED before its io_uring submit (the A2 ordering). Used by the
// submit-time-registration leg below.
bool produce_gated(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle           ctrl, slab;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok ||
       broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;

    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    pio::broadcast_ring::claim_result claim;
    if(ring.claim_with_policy(sizeof(k_payload), plexus::io::reliability::reliable,
                              plexus::io::congestion::block, claim) != pio::loan_status::ok)
        return false;
    std::memcpy(claim.slab.data(), &k_payload, sizeof(k_payload));
    if(ring.commit(claim.position, sizeof(k_payload)) != pio::loan_status::ok)
        return false;

    plexus::native::notifier_signal(ring.notify_generation(), ring.park_state());
    return true;
}

}

TEST_CASE("shm.notifier_bridge a producer wake reaches the user's asio reactor and drains",
          "[shm][notifier_bridge]")
{
    const std::string        fqn  = "topic.bridge." + std::to_string(::getpid());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 100; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);

        if(pid == 0)
        {
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ; // wait for the consumer to stamp the regions + arm
            const bool ok = produce(fqn);
            c->published.store(ok ? 1u : 2u, std::memory_order_release);
            ::_exit(ok ? 0 : 1);
        }

        // CONSUMER parent: create the regions, build the channel (its subscriber
        // registers a cursor at the tail), arm the bridge on a user-owned
        // io_context bound to the ring's notify word, then drive ONLY that reactor.
        posix_shm_region_broker broker;
        region_handle           ctrl, slab;
        REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count),
                              pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn),
                              pio::slab_region_bytes(geom.cell_count, geom.slot_capacity),
                              pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count,
                                            geom.slot_capacity, ring) == pio::loan_status::ok);

        ::asio::io_context                            io;
        plexus::asio::shm::ring_notifier<test_policy> bridge(io, ring.notify_generation());

        // The channel's subscriber registers its cursor at the producer's tail at
        // construction; build it BEFORE announcing ready so the cursor is fixed
        // before the producer publishes (no publish-before-arm race).
        pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>> channel(
                ring, bridge, plexus::io::reliability::reliable, plexus::io::congestion::block);

        std::uint32_t got = 0;
        // The arm()'d drain runs on a posted reactor turn: drain the channel and
        // capture the delivered value. This is the registry's drain-this-channel
        // callback shape (drain over the live subscriber).
        bridge.arm(
                [&]
                {
                    pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>>::deliver_fn
                            deliver = [&](plexus::wire_bytes<pio::shm_slot_owner> wb)
                    {
                        std::memcpy(&got, wb.data(), sizeof(got));
                        c->value_seen.store(1u, std::memory_order_release);
                    };
                    channel.drain(deliver);
                });

        c->regions_ready.store(1, std::memory_order_release);

        // Drive the user's reactor until the wake lands the drain (or a bounded
        // timeout so a missed wake surfaces as a failure, never a hang).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while(c->value_seen.load(std::memory_order_acquire) == 0 &&
              std::chrono::steady_clock::now() < deadline)
            io.run_for(std::chrono::milliseconds(20));

        bridge.disarm(); // stop the watch before the ring/regions unwind

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
        REQUIRE(c->value_seen.load(std::memory_order_acquire) == 1u);
        REQUIRE(got == k_payload);

        ::munmap(c, sizeof(coord));
    }
}

TEST_CASE("shm.notifier_bridge a gated signal wakes a parked io_uring waiter (submit-time "
          "registration)",
          "[shm][notifier_bridge][wake_gating][submit_futex]")
{
    // The A2 proof: the io_uring submit-time waiter registration closes the lost-wake
    // window for the GATED wake. The consumer's submit_futex_wait stores PARKED
    // (release) BEFORE io_uring_submit registers the waiter; the producer's TWO-ARG
    // gated signal reads park_state and issues the FUTEX_WAKE only because it observes
    // PARKED. If the kernel registered the waiter LATER than the submit (A2 false), the
    // producer could read a stale non-PARKED state, skip the wake, and the consumer
    // would never drain — surfacing as the deadline timeout below, never a silent hang.
    // Looped N>=100; the parked consumer drives the REAL submit_futex_wait path.
    const std::string        fqn  = "topic.bridgegate." + std::to_string(::getpid());
    const pio::ring_geometry geom = pio::ring_geometry_for(std::nullopt);

    for(int iter = 0; iter < 100; ++iter)
    {
        coord *c = map_coord();
        REQUIRE(c != nullptr);

        const pid_t pid = ::fork();
        REQUIRE(pid >= 0);
        if(pid == 0)
        {
            while(c->regions_ready.load(std::memory_order_acquire) == 0)
                ; // wait for the consumer to arm (PARKED stored before its io_uring submit)
            const bool ok = produce_gated(fqn);
            c->published.store(ok ? 1u : 2u, std::memory_order_release);
            ::_exit(ok ? 0 : 1);
        }

        posix_shm_region_broker broker;
        region_handle           ctrl, slab;
        REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count),
                              pio::create_options{}, ctrl) == pio::region_status::ok);
        REQUIRE(broker.create(slab_name(fqn),
                              pio::slab_region_bytes(geom.cell_count, geom.slot_capacity),
                              pio::create_options{}, slab) == pio::region_status::ok);

        pio::broadcast_ring ring;
        REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count,
                                            geom.slot_capacity, ring) == pio::loan_status::ok);

        ::asio::io_context io;
        // The GATED bridge: bound to BOTH the generation word AND the in-region park
        // word, so submit_futex_wait stores PARKED before its io_uring submit and the
        // producer's gated signal reads it.
        plexus::asio::shm::ring_notifier<test_policy> bridge(io, ring.notify_generation(),
                                                             ring.park_state());

        pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>> channel(
                ring, bridge, plexus::io::reliability::reliable, plexus::io::congestion::block);

        std::uint32_t got = 0;
        bridge.arm(
                [&]
                {
                    pio::shm_channel<plexus::asio::shm::ring_notifier<test_policy>>::deliver_fn
                            deliver = [&](plexus::wire_bytes<pio::shm_slot_owner> wb)
                    {
                        std::memcpy(&got, wb.data(), sizeof(got));
                        c->value_seen.store(1u, std::memory_order_release);
                    };
                    channel.drain(deliver);
                });

        // arm() ran submit_futex_wait: the consumer is now PARKED (the store preceded
        // the io_uring submit). Release the producer only now so its gated signal must
        // observe PARKED to land the wake.
        c->regions_ready.store(1, std::memory_order_release);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while(c->value_seen.load(std::memory_order_acquire) == 0 &&
              std::chrono::steady_clock::now() < deadline)
            io.run_for(std::chrono::milliseconds(20));

        bridge.disarm();

        int status = 0;
        while(::waitpid(pid, &status, 0) < 0)
            ;
        REQUIRE(WIFEXITED(status));
        REQUIRE(WEXITSTATUS(status) == 0);
        REQUIRE(c->value_seen.load(std::memory_order_acquire) == 1u); // the gated wake landed
        REQUIRE(got == k_payload);

        ::munmap(c, sizeof(coord));
    }
}

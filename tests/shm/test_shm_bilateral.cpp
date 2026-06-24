// over-limit: one cohesive bilateral demand-driven convergence proof (xproc); the cells share
// the one two-process independent-acquire harness over a converged named ring, and that shared
// fixture preamble cannot split across TUs without scattering that shared cross-process state.
#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/shm/ring_geometry.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/shm_selection.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The bilateral, consumer-sovereign shared-memory upgrade via DEMAND-DRIVEN
// CONVERGENCE. Two real processes over the POSIX broker each decide INDEPENDENTLY
// whether to attempt the SHM acquire from their OWN dispatch hint
// (attempt_shm_upgrade) — NOTHING about the hint
// is exchanged on the wire. When both attempt, they converge on the SAME
// deterministically-named ring (region_name_for) the same way they converge on the
// name, and a value flows over shared memory. The cases proved:
//   (a) a hint that sizes the ring (the publisher's max_payload) lets a wide value fit;
//   (b) a hint with NO max_payload upgrades on the DEFAULT geometry (the
//       subscriber-only sizing fallback — the consumer rescues itself);
//   (c) neither end's hint qualifies -> neither attempts the acquire (no SHM).
// Looped in-body; the ctest binary is re-run >=3 process runs for reproducibility.

namespace pio = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

namespace {

struct coord
{
    std::atomic<std::uint32_t> publisher_ready{0};
    std::atomic<std::uint32_t> subscriber_armed{0};
    std::atomic<std::uint32_t> subscriber_done{0};
    std::atomic<std::uint32_t> value_seen{0};
    std::atomic<std::uint32_t> value_got{0};
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

// The subscriber process: it attaches the converged ring (creating the
// default-geometry ring itself if the publisher has not minted one yet — the
// consumer-sovereign self-rescue), registers a cursor at the tail, and consumes the
// one published value. It NEVER learns the publisher's hint or max_payload; it acts
// on its OWN upgrade decision and the deterministic name alone.
bool subscribe_shm(const std::string &fqn, coord *c, std::uint32_t want_payload)
{
    posix_shm_region_broker broker;
    region_handle           ctrl, slab;

    // Wait for the publisher to mint the ring, then attach the SAME names.
    while(c->publisher_ready.load(std::memory_order_acquire) == 0)
        ;
    if(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok ||
       broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
        return false;
    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    std::uint32_t idx = 0;
    if(ring.register_cursor(idx) != pio::loan_status::ok)
        return false;
    std::uint64_t cursor = ring.tail_position();
    c->subscriber_armed.store(1, std::memory_order_release);

    bool ok = false;
    for(;;)
    {
        pio::broadcast_ring::consume_result out;
        const auto                          st = ring.consume(cursor, out);
        if(st == pio::loan_status::ok)
        {
            std::uint32_t got = 0;
            std::memcpy(&got, out.slab.data(), sizeof(got));
            c->value_got.store(got, std::memory_order_release);
            ok = (got == want_payload);
            break;
        }
        if(st == pio::loan_status::congested)
            ++cursor;
    }
    ring.unregister_cursor(idx);
    return ok;
}

// A publisher-driven SHM round-trip: the publisher (parent) mints + sizes the ring
// (max_payload, 0 -> default), the subscriber (child) converges + consumes. Returns
// whether the value crossed over shared memory. region_name_for is the only thing
// either end shares; the hint never rides the wire.
bool run_shm_roundtrip(const std::string &fqn, std::uint32_t publisher_max_payload,
                       std::uint32_t payload)
{
    coord *c = map_coord();
    REQUIRE(c != nullptr);

    const pid_t pid = ::fork();
    REQUIRE(pid >= 0);
    if(pid == 0)
    {
        const bool ok = subscribe_shm(fqn, c, payload);
        c->value_seen.store(ok ? 1u : 0u, std::memory_order_release);
        c->subscriber_armed.store(1, std::memory_order_release);
        c->subscriber_done.store(1, std::memory_order_release);
        ::_exit(ok ? 0 : 1);
    }

    // The publisher mints + sizes the ring on the demand-driven path: the same
    // region_name_for naming + max_payload geometry the shm_topic_registry::acquire
    // uses internally (0 -> the default geometry). The direction is request (a pub/sub
    // topic). Both ends derive the names from the fqn alone — the convergence.
    posix_shm_region_broker            broker;
    region_handle                      ctrl, slab;
    const std::optional<std::uint32_t> want = publisher_max_payload == 0
            ? std::nullopt
            : std::optional<std::uint32_t>{publisher_max_payload};
    const pio::ring_geometry           geom = pio::ring_geometry_for(want);
    REQUIRE(broker.create(control_name(fqn), pio::control_region_bytes(geom.cell_count),
                          pio::create_options{}, ctrl) == pio::region_status::ok);
    REQUIRE(broker.create(slab_name(fqn),
                          pio::slab_region_bytes(geom.cell_count, geom.slot_capacity),
                          pio::create_options{}, slab) == pio::region_status::ok);
    pio::broadcast_ring ring;
    REQUIRE(pio::broadcast_ring::create(ctrl.bytes(), slab.bytes(), geom.cell_count,
                                        geom.slot_capacity, ring) == pio::loan_status::ok);
    c->publisher_ready.store(1, std::memory_order_release);

    while(c->subscriber_armed.load(std::memory_order_acquire) == 0)
        ;
    pio::broadcast_ring::claim_result claim;
    REQUIRE(ring.claim_with_policy(sizeof(payload), plexus::io::reliability::reliable,
                                   plexus::io::congestion::block, claim) == pio::loan_status::ok);
    std::memcpy(claim.slab.data(), &payload, sizeof(payload));
    REQUIRE(ring.commit(claim.position, sizeof(payload)) == pio::loan_status::ok);

    while(c->subscriber_done.load(std::memory_order_acquire) == 0)
        ;
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        ;
    const bool seen = c->value_seen.load(std::memory_order_acquire) == 1u;
    ::munmap(c, sizeof(coord));
    return WIFEXITED(status) && WEXITSTATUS(status) == 0 && seen;
}

}

TEST_CASE("shm.bilateral demand-driven convergence: the hint gates each side's acquire attempt, no "
          "wire exchange",
          "[shm][bilateral]")
{
    using pio::attempt_shm_upgrade;
    using plexus::io::dispatch_hint;

    // The decision is purely local: same-host AND own-hint. NOTHING about the hint
    // rides the wire — both ends run THIS over their own state and converge on the
    // named ring. A non-same-host pair never upgrades regardless of the hint.
    SECTION("either end's qualifying hint gates its OWN attempt; neither hint -> no attempt")
    {
        // (a) a PUBLISHER end with a qualifying hint attempts the acquire (it drives
        // the create/size); (b) a SUBSCRIBER end with a qualifying hint attempts it
        // (the consumer-sovereign self-rescue) -- EITHER end's hint upgrades that end.
        CHECK(attempt_shm_upgrade(true,
                                  dispatch_hint::frequent)); // publisher OR subscriber, hinted
        CHECK(attempt_shm_upgrade(true, dispatch_hint::large));
        // (c) neither end hinted -> neither attempts (stays on the wire).
        CHECK_FALSE(attempt_shm_upgrade(true, dispatch_hint::none));
        // not same-host -> never, even with a hint (the eligibility gate).
        CHECK_FALSE(attempt_shm_upgrade(false, dispatch_hint::frequent));
    }

    // The ring-sizing authority: the publisher direction (request) carries the
    // max_payload; a subscriber-only upgrade (response direction here standing in for
    // the no-publisher-sizing case) gets 0 -> the default geometry.
    SECTION("ring-sizing authority: publisher sizes via max_payload, subscriber-only falls back to "
            "default")
    {
        CHECK(pio::upgrade_ring_max_payload(pio::ring_direction::request, 8192u) == 8192u);
        CHECK(pio::upgrade_ring_max_payload(pio::ring_direction::response, 8192u) == 0u);
    }
}

TEST_CASE("shm.bilateral a publisher-sized ring lets a wide value cross over shared memory (xproc)",
          "[shm][bilateral]")
{
    // The publisher declares max_payload; the ring is sized to it; a value at that
    // width fits and crosses to the converged subscriber. Looped + reproduced.
    const std::string       fqn       = "topic.bilateral.sized." + std::to_string(::getpid());
    constexpr std::uint32_t k_payload = 0xBADC0FFEu;
    for(int iter = 0; iter < 3; ++iter)
        REQUIRE(run_shm_roundtrip(fqn + "." + std::to_string(iter), /*max_payload=*/8192u,
                                  k_payload));
}

TEST_CASE("shm.bilateral a subscriber-only upgrade with no max_payload uses the default geometry "
          "(xproc)",
          "[shm][bilateral]")
{
    // No publisher max_payload (0) -> the default ring geometry. The value still
    // crosses: the consumer-sovereign upgrade does not need the publisher to size.
    const std::string       fqn       = "topic.bilateral.default." + std::to_string(::getpid());
    constexpr std::uint32_t k_payload = 0x5AFE5A1Du;
    for(int iter = 0; iter < 3; ++iter)
        REQUIRE(run_shm_roundtrip(fqn + "." + std::to_string(iter), /*max_payload=*/0u, k_payload));
}

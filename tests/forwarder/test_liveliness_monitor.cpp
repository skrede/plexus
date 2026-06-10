// The liveliness_monitor unit oracle: the ONE router-level periodic ticker driven
// by the deterministic virtual clock (test_clock advance + inproc_executor drain),
// never a wall-clock sleep. It pins the single hardest correctness property — the
// edge-latched single-fire-per-lapse with re-arm-on-resume — plus the two-stamp
// distinctness (a heartbeat refreshes the lease but NOT the deadline), the inert
// 0-period, the no-fire-on-a-deregistered-endpoint lifetime invariant, and the
// zero-allocation steady-state stamp. Every timing assertion is looped for
// reproducibility (a timing feature is never declared from a single run), and every
// period is pinned >= k_tick_granularity so an advance actually crosses a tick
// expiry.

#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveness_event.h"

#include "plexus/testing/test_clock.h"
#include "plexus/testing/mock_policy.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/node_id.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using plexus::node_id;
using plexus::io::liveness_event;
using plexus::io::liveness_kind;
using plexus::io::k_tick_granularity;
using plexus::testing::test_clock;
using plexus::testing::mock_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;

using monitor = plexus::io::liveliness_monitor<mock_policy, test_clock>;

namespace {

// A period pinned a tick above the granularity: an advance(P + a tick) reliably
// crosses a tick expiry so the scan fires. (M1: a sub-granularity period would never
// cross a tick boundary and the test would stick — never fire.)
constexpr auto k_period = k_tick_granularity * 3;   // 300ms
constexpr auto k_lease  = k_tick_granularity * 5;   // 500ms
static_assert(k_period >= k_tick_granularity, "a deadline period below the tick granularity never crosses a tick");
static_assert(k_lease >= k_tick_granularity, "a lease below the tick granularity never crosses a tick");

constexpr int k_loops = 50;

node_id make_id(std::uint8_t seed)
{
    node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// A counting sink for fired events, separated by kind.
struct event_sink
{
    int missed_deadline = 0;
    int lease_expired = 0;

    void attach(monitor &m)
    {
        m.on_liveness([this](const liveness_event &ev) {
            if(ev.kind == liveness_kind::missed_deadline)
                ++missed_deadline;
            else
                ++lease_expired;
        });
    }
};

}

TEST_CASE("liveliness monitor: a data gap beyond the deadline period fires exactly one missed-deadline")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock> bus;
        inproc_executor<test_clock> ex{bus};

        monitor m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x11);
        const std::uint64_t topic_hash = 0xABCDull;
        m.register_endpoint(id, topic_hash,
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()), 0);

        m.stamp_data(id, topic_hash);

        // A short gap (under the period) fires nothing.
        test_clock::advance(k_tick_granularity);
        ex.drain();
        REQUIRE(sink.missed_deadline == 0);

        // Resume, then lapse past the period: exactly ONE missed-deadline.
        m.stamp_data(id, topic_hash);
        test_clock::advance(k_period + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.missed_deadline == 1);

        // A further tick while still lapsed fires NO second event (edge-latched).
        test_clock::advance(k_tick_granularity);
        ex.drain();
        REQUIRE(sink.missed_deadline == 1);

        // Resume clears the latch, a second lapse re-fires.
        m.stamp_data(id, topic_hash);
        test_clock::advance(k_period + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.missed_deadline == 2);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("liveliness monitor: a presence gap beyond the lease fires exactly one lease-expiry; a heartbeat keeps it alive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock> bus;
        inproc_executor<test_clock> ex{bus};

        monitor m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x22);
        m.register_endpoint(id, 0xABCDull, 0,
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));

        m.stamp_seen(id);

        // Half the lease, then a heartbeat refreshes presence: no expiry.
        test_clock::advance(k_lease / 2 + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.lease_expired == 0);
        m.stamp_seen(id);
        test_clock::advance(k_lease / 2 + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.lease_expired == 0);

        // Now silence past the lease: exactly one expiry.
        test_clock::advance(k_lease + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.lease_expired == 1);

        // Still silent: no second fire (edge-latched).
        test_clock::advance(k_tick_granularity);
        ex.drain();
        REQUIRE(sink.lease_expired == 1);

        // A resumed heartbeat clears the latch, a later lapse re-fires.
        m.stamp_seen(id);
        test_clock::advance(k_lease + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.lease_expired == 2);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("liveliness monitor: the two stamps are distinct — a heartbeat refreshes the lease but NOT the deadline")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock> bus;
        inproc_executor<test_clock> ex{bus};

        monitor m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x33);
        const std::uint64_t topic_hash = 0xBEEFull;
        // L > P so the deadline lapses first; both periods >= the granularity.
        m.register_endpoint(id, topic_hash,
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()),
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));

        m.stamp_data(id, topic_hash);

        // Advance past the deadline period (but under the lease), feeding only
        // heartbeats — they refresh presence but NOT the data clock.
        test_clock::advance(k_period + k_tick_granularity);
        m.stamp_seen(id);   // a heartbeat: keeps the lease alive, deadline already lapsed
        ex.drain();

        // The deadline fired (data lapsed); the lease did NOT (the heartbeat kept presence).
        REQUIRE(sink.missed_deadline == 1);
        REQUIRE(sink.lease_expired == 0);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("liveliness monitor: a 0 period is inert (never fires)")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock> bus;
        inproc_executor<test_clock> ex{bus};

        monitor m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x44);
        m.register_endpoint(id, 0xABCDull, 0, 0);   // neither axis requested

        // Advance far with no stamps: a not-requested axis is skipped, never a false fire.
        test_clock::advance(k_lease * 4);
        ex.drain();
        REQUIRE(sink.missed_deadline == 0);
        REQUIRE(sink.lease_expired == 0);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("liveliness monitor: a deregistered endpoint never fires (no resurrection)")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock> bus;
        inproc_executor<test_clock> ex{bus};

        monitor m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x55);
        const std::uint64_t topic_hash = 0xCAFEull;
        m.register_endpoint(id, topic_hash,
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()),
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));
        m.stamp_data(id, topic_hash);

        // Tear the endpoint down BEFORE the lapse: a fire reads only resident state,
        // and both maps no longer hold it — so nothing ever fires.
        m.deregister_endpoint(id);
        test_clock::advance(k_lease + k_tick_granularity);
        ex.drain();
        REQUIRE(sink.missed_deadline == 0);
        REQUIRE(sink.lease_expired == 0);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("liveliness monitor: a stamp allocates nothing in steady state")
{
    test_clock::reset();
    inproc_bus<test_clock> bus;
    inproc_executor<test_clock> ex{bus};

    monitor m{ex};
    m.start();

    const node_id id = make_id(0x66);
    const std::uint64_t topic_hash = 0xD00Dull;
    // Warm the maps once (register grows the entries).
    m.register_endpoint(id, topic_hash,
                        static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()),
                        static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));

    plexus::testing::reset_alloc_count();
    for(int i = 0; i < 1000; ++i)
    {
        m.stamp_data(id, topic_hash);
        m.stamp_seen(id);
    }
    REQUIRE(plexus::testing::alloc_count() == 0);
}

#include "test_liveliness_monitor_common.h"

#include "support/alloc_counter.h"

using namespace liveliness_monitor_fixture;

TEST_CASE("liveliness monitor: a 0 period is inert (never fires)")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        test_clock::reset();
        inproc_bus<test_clock>      bus;
        inproc_executor<test_clock> ex{bus};

        monitor    m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id id = make_id(0x44);
        m.register_endpoint(id, 0xABCDull, 0, 0); // neither axis requested

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
        inproc_bus<test_clock>      bus;
        inproc_executor<test_clock> ex{bus};

        monitor    m{ex};
        event_sink sink;
        sink.attach(m);
        m.start();

        const node_id       id         = make_id(0x55);
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
    inproc_bus<test_clock>      bus;
    inproc_executor<test_clock> ex{bus};

    monitor m{ex};
    m.start();

    const node_id       id         = make_id(0x66);
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

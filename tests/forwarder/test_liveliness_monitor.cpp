#include "test_liveliness_monitor_common.h"

using namespace liveliness_monitor_fixture;

TEST_CASE("liveliness monitor: a data gap beyond the deadline period fires exactly one "
          "missed-deadline")
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

        const node_id id               = make_id(0x11);
        const std::uint64_t topic_hash = 0xABCDull;
        m.register_endpoint(id, topic_hash, static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()), 0);

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

TEST_CASE("liveliness monitor: a presence gap beyond the lease fires exactly one lease-expiry; a "
          "heartbeat keeps it alive")
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
        m.register_endpoint(id, 0xABCDull, 0, static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));

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

TEST_CASE("liveliness monitor: the two stamps are distinct - a heartbeat refreshes the lease but "
          "NOT the deadline")
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

        const node_id id               = make_id(0x33);
        const std::uint64_t topic_hash = 0xBEEFull;
        // L > P so the deadline lapses first; both periods >= the granularity.
        m.register_endpoint(id, topic_hash, static_cast<std::uint64_t>(std::chrono::nanoseconds(k_period).count()),
                            static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count()));

        m.stamp_data(id, topic_hash);

        // Advance past the deadline period (but under the lease), feeding only
        // heartbeats — they refresh presence but NOT the data clock.
        test_clock::advance(k_period + k_tick_granularity);
        m.stamp_seen(id); // a heartbeat: keeps the lease alive, deadline already lapsed
        ex.drain();

        // The deadline fired (data lapsed); the lease did NOT (the heartbeat kept presence).
        REQUIRE(sink.missed_deadline == 1);
        REQUIRE(sink.lease_expired == 0);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

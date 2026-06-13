// The subscriber-qos thread oracle: the SUBSCRIBING node must store its OWN requested
// deadline/lease so the local liveliness monitor reads real periods at the register
// seam (not a defaulted 0 that would make the timing gate vacuous). This pins the
// store end of the thread — attach(peer, fqn, qos) lands the qos in the registry so
// qos_for_subscriber reads it back — and the durable-demand end — remember_demand
// carries the qos so a deferred dial resurrects the real periods through
// remembered_topics.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::io::subscriber_qos;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

TEST_CASE("forwarder: a subscriber qos thread stores the requested periods in the registry")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};

    forwarder fwd{};
    inproc_channel<> ch{ex};
    forwarder::peer peer{ch, "node-a"};

    // A subscriber with NON-default requested periods (a real choice, not 0).
    const subscriber_qos qos{.requested_deadline_ns = 300'000'000ull,
                             .requested_lease_ns = 500'000'000ull};

    REQUIRE(fwd.attach(peer, "alpha", qos));

    // The durable-demand record carries the qos so a deferred dial resurrects the
    // SAME periods (not a default) on reconnect.
    const auto &demand = fwd.remembered_topics("node-a");
    REQUIRE(demand.size() == 1);
    REQUIRE(demand.front().fqn == "alpha");
    REQUIRE(demand.front().qos.requested_deadline_ns == 300'000'000ull);
    REQUIRE(demand.front().qos.requested_lease_ns == 500'000'000ull);
}

TEST_CASE("registry: add_subscriber stores the subscriber qos and qos_for_subscriber reads it back")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_channel<> ch{ex};

    plexus::io::subscriber_registry<inproc_channel<>> registry;
    const std::uint64_t hash = plexus::wire::fqn_topic_hash("alpha");
    const subscriber_qos qos{.requested_deadline_ns = 123'456'789ull,
                             .requested_lease_ns = 987'654'321ull};

    registry.add_subscriber(hash, "alpha", ch, "node-a", qos);

    const subscriber_qos stored = registry.qos_for_subscriber(hash, ch);
    REQUIRE(stored.requested_deadline_ns == 123'456'789ull);
    REQUIRE(stored.requested_lease_ns == 987'654'321ull);

    // An unknown (topic, channel) reads back the friendly default — a genuine,
    // tested absence (0 periods), never a false-armed monitor.
    const subscriber_qos absent = registry.qos_for_subscriber(0xDEAD, ch);
    REQUIRE(absent.requested_deadline_ns == 0ull);
    REQUIRE(absent.requested_lease_ns == 0ull);
}

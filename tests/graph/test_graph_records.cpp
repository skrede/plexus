#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>

using plexus::graph::observation;
using plexus::graph::participant_record;
using plexus::graph::provenance;
using plexus::graph::route;
using plexus::graph::topic_record;

TEST_CASE("records keep observation values mutually distinct", "[graph][records]")
{
    STATIC_REQUIRE(observation::directly_observed != observation::reported);
}

TEST_CASE("records reserve no reporter for a direct-only provenance", "[graph][records]")
{
    constexpr provenance origin{observation::directly_observed, std::nullopt};
    STATIC_REQUIRE(origin.how == observation::directly_observed);
    STATIC_REQUIRE(origin.reporter == std::nullopt);
}

TEST_CASE("records reserve no via for a direct-only route", "[graph][records]")
{
    const route reach{plexus::io::endpoint{"udp", "203.0.113.7:9000"}, std::nullopt};
    REQUIRE(reach.via == std::nullopt);
}

TEST_CASE("records separate identity, reachability, and provenance", "[graph][records]")
{
    const plexus::node_id id{};
    const participant_record rec{
        id,
        route{plexus::io::endpoint{"udp", "203.0.113.7:9000"}, std::nullopt},
        provenance{observation::directly_observed, std::nullopt}};

    REQUIRE(rec.id == id);
    REQUIRE(rec.reach.via == std::nullopt);
    REQUIRE(rec.origin.how == observation::directly_observed);
    REQUIRE(rec.origin.reporter == std::nullopt);
}

TEST_CASE("records leave an undeclared topic type absent", "[graph][records]")
{
    const topic_record topic{"sensor/imu", std::nullopt};
    REQUIRE(topic.type == std::nullopt);
}

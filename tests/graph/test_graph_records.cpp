#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>

using plexus::node_id;
using plexus::graph::observation;
using plexus::graph::participant_record;
using plexus::graph::provenance;
using plexus::graph::route;
using plexus::graph::topic_record;
using plexus::graph::topic_role;
using plexus::graph::type_name_list;

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
    const topic_record topic{node_id{}, "sensor/imu", type_name_list{}, topic_role::publisher, false};
    REQUIRE(topic.types.count == 0);
    REQUIRE_FALSE(topic.truncated);
}

TEST_CASE("records keep a declared-empty topic type distinct from an undeclared one", "[graph][records]")
{
    const topic_record declared_empty{node_id{}, "sensor/imu", type_name_list{{""}, 1}, topic_role::publisher, false};
    REQUIRE(declared_empty.types.count == 1);
    REQUIRE(declared_empty.types.names[0].empty());
}

TEST_CASE("records carry a declared topic type by name", "[graph][records]")
{
    const topic_record topic{node_id{}, "sensor/imu", type_name_list{{"sensor_msgs/Imu"}, 1}, topic_role::subscriber, false};
    REQUIRE(topic.types.count == 1);
    REQUIRE(topic.types.names[0] == "sensor_msgs/Imu");
    REQUIRE(topic.role == topic_role::subscriber);
}

TEST_CASE("records surface a polytype topic as more than one distinct name", "[graph][records]")
{
    const topic_record topic{node_id{}, "sensor/imu", type_name_list{{"sensor_msgs/Imu", "custom/Imu"}, 2}, topic_role::publisher, false};
    REQUIRE(topic.types.count == 2);
    REQUIRE(topic.types.count > 1);
    REQUIRE(topic.types.names[1] == "custom/Imu");
}

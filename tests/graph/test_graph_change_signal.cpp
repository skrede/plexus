// The change-detection contract on the topic-table mutators: upsert reports changed==true only on
// a genuinely new (participant, topic, role) edge OR a new distinct type_id, and changed==false on
// a duplicate declare or a reject-and-count drop; remove_edge / remove_node report whether they
// actually erased. Both storage twins carry the IDENTICAL result surface — INV-1 symmetric. The
// upsert_result still converts to upsert_outcome so the reject-and-count oracle keeps compiling.
// Header-only core, Catch2 main only.

#include "plexus/graph/topic_type_table.h"
#include "plexus/graph/fixed_topic_storage.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::graph::basic_topic_type_table;
using plexus::graph::fixed_topic_storage;
using plexus::graph::k_topic_type_list_cap;
using plexus::graph::topic_edge;
using plexus::graph::topic_role;
using plexus::graph::topic_type_table;
using plexus::graph::upsert_outcome;

namespace {

constexpr std::size_t k_topics = 2;

using mcu_table = basic_topic_type_table<fixed_topic_storage<k_topics>>;

plexus::node_id make_id(std::uint8_t tag)
{
    plexus::node_id id{};
    id[0] = std::byte{tag};
    return id;
}

topic_edge publisher_of(std::uint8_t tag, std::string_view topic, std::string_view type_name, std::uint64_t type_id)
{
    return topic_edge{make_id(tag), topic, type_name, type_id, topic_role::publisher};
}

// The change-signal contract, run against any basic_topic_type_table<Storage>.
template<typename Table>
void exercise_change_signal(Table &table)
{
    // A genuinely new edge is a change.
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7)).changed == true);
    // A fully duplicate edge+type is not.
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7)).changed == false);
    // A new distinct type on the SAME edge is a change (type absent->known).
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "custom/Imu", 9)).changed == true);
    // The same edge re-declaring an already-known type is not.
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "custom/Imu", 9)).changed == false);
    // A second participant on the topic is a new edge -> a change.
    REQUIRE(table.upsert(publisher_of(0x02, "sensor/imu", "sensor_msgs/Imu", 7)).changed == true);

    // Removing a present edge is a change; removing an absent one is not.
    REQUIRE(table.remove_edge(make_id(0x02), "sensor/imu", topic_role::publisher) == true);
    REQUIRE(table.remove_edge(make_id(0x02), "sensor/imu", topic_role::publisher) == false);
    REQUIRE(table.remove_node(make_id(0x01)) == true);
    REQUIRE(table.remove_node(make_id(0x01)) == false);
}

} // namespace

TEST_CASE("graph_change_signal: the fixed twin reports change only on a real edge/type/remove", "[graph][topic_table]")
{
    mcu_table table;
    exercise_change_signal(table);
}

TEST_CASE("graph_change_signal: the default twin carries the identical change signal", "[graph][topic_table]")
{
    topic_type_table table;
    exercise_change_signal(table);
}

TEST_CASE("graph_change_signal: a reject-and-count drop is not a change", "[graph][topic_table]")
{
    mcu_table table;
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "T", 1)).changed == true);
    REQUIRE(table.upsert(publisher_of(0x02, "sensor/gps", "T", 1)).changed == true);
    // The (Topics+1)-th topic has no room: dropped, and a drop is not a change.
    const auto over = table.upsert(publisher_of(0x03, "sensor/lidar", "T", 1));
    REQUIRE(over.changed == false);
    REQUIRE(static_cast<upsert_outcome>(over) == upsert_outcome::dropped);
    REQUIRE(table.dropped() == 1);
}

TEST_CASE("graph_change_signal: a truncated-with-new-edge upsert is still a change", "[graph][topic_table]")
{
    mcu_table table;
    for(std::size_t i = 0; i < k_topic_type_list_cap; ++i)
        table.upsert(publisher_of(static_cast<std::uint8_t>(0x10 + i), "sensor/imu", "T", 1 + i));
    // A fresh participant whose extra distinct type overruns the cap: the new edge is a change even
    // though the type list truncates.
    const auto edge = table.upsert(publisher_of(0x20, "sensor/imu", "overflow/T", 99));
    REQUIRE(edge.changed == true);
    REQUIRE(static_cast<upsert_outcome>(edge) == upsert_outcome::truncated);
}

TEST_CASE("graph_change_signal: upsert_result implicitly converts to upsert_outcome", "[graph][topic_table]")
{
    mcu_table table;
    const upsert_outcome outcome = table.upsert(publisher_of(0x01, "sensor/imu", "T", 1));
    REQUIRE(outcome == upsert_outcome::stored);
}

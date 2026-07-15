#include "support/alloc_counter.h"

#include "plexus/graph/topic_type_table.h"
#include "plexus/graph/fixed_topic_storage.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::graph::basic_topic_type_table;
using plexus::graph::fixed_topic_storage;
using plexus::graph::k_topic_type_list_cap;
using plexus::graph::topic_edge;
using plexus::graph::topic_record;
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

}

TEST_CASE("topic_table: surfaces two publisher types of one topic as a polytype conflict", "[graph][topic_table]")
{
    mcu_table table;
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7)) == upsert_outcome::stored);
    REQUIRE(table.upsert(publisher_of(0x02, "sensor/imu", "custom/Imu", 9)) == upsert_outcome::stored);

    std::size_t records = 0;
    table.for_each([&](const topic_record &r) {
        ++records;
        REQUIRE(r.types.count == 2);
        REQUIRE(r.types.names[0] == "sensor_msgs/Imu");
        REQUIRE(r.types.names[1] == "custom/Imu");
    });
    REQUIRE(records == 2);
}

TEST_CASE("topic_table: leaves two publishers agreeing on one type conflict-free", "[graph][topic_table]")
{
    mcu_table table;
    table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7));
    table.upsert(publisher_of(0x02, "sensor/imu", "sensor_msgs/Imu", 7));

    std::size_t records = 0;
    table.for_each([&](const topic_record &r) {
        ++records;
        REQUIRE(r.types.count == 1);
    });
    REQUIRE(records == 2);
}

TEST_CASE("topic_table: the default table agrees with the fixed twin on the conflict marker", "[graph][topic_table]")
{
    topic_type_table table;
    table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7));
    table.upsert(publisher_of(0x02, "sensor/imu", "custom/Imu", 9));

    std::size_t records = 0;
    table.for_each([&](const topic_record &r) {
        ++records;
        REQUIRE(r.types.count == 2);
    });
    REQUIRE(records == 2);
}

TEST_CASE("topic_table: reject-and-counts a topic past capacity without evicting a known one", "[graph][topic_table]")
{
    mcu_table table;
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", "T", 1)) == upsert_outcome::stored);
    REQUIRE(table.upsert(publisher_of(0x02, "sensor/gps", "T", 1)) == upsert_outcome::stored);
    REQUIRE(table.upsert(publisher_of(0x03, "sensor/lidar", "T", 1)) == upsert_outcome::dropped);
    REQUIRE(table.dropped() == 1);

    std::size_t records = 0;
    table.for_each([&](const topic_record &r) {
        ++records;
        REQUIRE(r.name != "sensor/lidar");
    });
    REQUIRE(records == k_topics);
}

TEST_CASE("topic_table: flags a topic whose distinct types outrun the cap and keeps it enumerable", "[graph][topic_table]")
{
    mcu_table table;
    for(std::size_t i = 0; i < k_topic_type_list_cap; ++i)
        REQUIRE(table.upsert(publisher_of(static_cast<std::uint8_t>(0x10 + i), "sensor/imu", "T", 1 + i)) == upsert_outcome::stored);
    REQUIRE(table.upsert(publisher_of(0x20, "sensor/imu", "overflow/T", 99)) == upsert_outcome::truncated);
    REQUIRE(table.truncations() == 1);

    std::size_t records = 0;
    table.for_each([&](const topic_record &r) {
        ++records;
        REQUIRE(r.truncated);
        REQUIRE(r.name == "sensor/imu");
        REQUIRE(r.role == topic_role::publisher);
        REQUIRE(r.types.count == k_topic_type_list_cap);
    });
    REQUIRE(records == k_topic_type_list_cap + 1);
}

TEST_CASE("topic_table: clips an over-long type name and flags what it clipped", "[graph][topic_table]")
{
    const std::string name(plexus::wire::detail::k_max_type_name + 1, 'x');

    mcu_table table;
    REQUIRE(table.upsert(publisher_of(0x01, "sensor/imu", name, 7)) == upsert_outcome::truncated);
    REQUIRE(table.truncations() == 1);

    table.for_each([&](const topic_record &r) {
        REQUIRE(r.truncated);
        REQUIRE(r.types.count == 1);
        REQUIRE(r.types.names[0].size() == plexus::wire::detail::k_max_type_name);
    });
}

TEST_CASE("topic_table: refuses an over-long topic name rather than alias it onto another topic", "[graph][topic_table]")
{
    const std::string topic(plexus::wire::detail::k_max_fqn + 1, 'x');

    mcu_table table;
    REQUIRE(table.upsert(publisher_of(0x01, topic, "T", 7)) == upsert_outcome::dropped);
    REQUIRE(table.dropped() == 1);

    std::size_t records = 0;
    table.for_each([&](const topic_record &) { ++records; });
    REQUIRE(records == 0);
}

TEST_CASE("topic_table: sweeps a populated table without allocating", "[graph][topic_table]")
{
    mcu_table table;
    table.upsert(publisher_of(0x01, "sensor/imu", "sensor_msgs/Imu", 7));
    table.upsert(publisher_of(0x02, "sensor/imu", "custom/Imu", 9));
    table.upsert(topic_edge{make_id(0x03), "sensor/imu", "custom/Imu", 9, topic_role::subscriber});

    std::size_t names = 0;

    const std::size_t before = plexus::testing::alloc_count();
    table.for_each([&](const topic_record &r) { names += r.types.count; });
    const std::size_t after = plexus::testing::alloc_count();

    REQUIRE(names == 6);
    REQUIRE(after - before == 0);
}

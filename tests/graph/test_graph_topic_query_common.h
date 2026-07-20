#ifndef HPP_GUARD_PLEXUS_TESTS_GRAPH_TOPIC_QUERY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_GRAPH_TOPIC_QUERY_COMMON_H

#include "test_graph_topics_common.h"

#include "plexus/match/key_pattern.h"

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

namespace graph_topics_fixture {

using plexus::graph::snapshot_result;
using plexus::match::key_pattern;

constexpr const char *k_rival_type_name  = "sensor/Pose";
constexpr std::uint64_t k_rival_type_id  = 0x5150u;

// The rival declaration: the same value type under a different type identity, so one topic carries
// two publishers that disagree about it. It declares through the real publisher handle — a poked
// forwarder would reach the peers but never the declaring node's own bookkeeping.
struct pose_codec : imu_codec
{
    plexus::type_identity type_info() const
    {
        return {k_rival_type_id, k_rival_type_name};
    }
};

using pose_publisher = plexus::publisher<pose_codec>;

inline key_pattern pattern(std::string_view text)
{
    const auto made = key_pattern::make(text);
    REQUIRE(made.has_value());
    return *made;
}

// The type list carries no defined order — it is the order the declarations happened to be folded
// in, which two nodes seeing the same pair of publishers need not agree on. Sorted, so an assertion
// pins the SET of names a topic disagrees over rather than an arrival race.
inline std::vector<std::string> names_of(const topic_record &rec)
{
    std::vector<std::string> out;
    for(std::size_t i = 0; i < rec.types.count; ++i)
        out.emplace_back(rec.types.names[i]);
    std::sort(out.begin(), out.end());
    return out;
}

inline std::vector<std::string> sorted(std::vector<std::string> names)
{
    std::sort(names.begin(), names.end());
    return names;
}

inline const topic_record *find_record(std::span<const topic_record> buffer, std::size_t filled, std::string_view topic, const plexus::node_id &node)
{
    const auto view = buffer.first(filled);
    const auto it   = std::find_if(view.begin(), view.end(), [&](const topic_record &rec) { return rec.name == topic && rec.node == node; });
    return it == view.end() ? nullptr : &*it;
}

// Three nodes, because a two-node fixture cannot express a disagreement seen from the outside: the
// polytype cases need both rival publishers remote to one enumerating node AND local to another.
struct trio : net
{
    plexus::node_id id_c{make_id(0x0C)};
    plexus::inproc::inproc_transport<> tc{h.ex, h.bus};
    inproc_node c{h.ex, h.disc, id_c, tc, opts_for(true)};

    // A publishes the topic as one type, C as another, C also publishes an untyped topic, and B
    // demands the contested one. Every endpoint is a real handle driving the node's own seams, so
    // each node's table carries its own edges alongside the ones its peers declared.
    void declare_all()
    {
        connect();
        c.listen({"inproc", "host-c:7000"});
        drive();
        REQUIRE(c.router().is_connected(id_b));

        m_imu   = std::make_unique<imu_publisher>(a, k_imu_topic, plexus::typed_publisher_options{}, imu_codec{});
        m_rival = std::make_unique<pose_publisher>(c, k_imu_topic, plexus::typed_publisher_options{}, pose_codec{});
        m_plain = std::make_unique<plexus::publisher<void>>(c, k_plain_topic);
        m_demand = std::make_unique<imu_subscriber>(b, k_imu_topic, [](const std::uint32_t &) {});
        drive();
    }

    // Drops A's publisher handle: the retire path the node's own edge rides out on.
    void retire_imu_publisher()
    {
        m_imu.reset();
        drive();
    }

private:
    std::unique_ptr<imu_publisher> m_imu;
    std::unique_ptr<pose_publisher> m_rival;
    std::unique_ptr<plexus::publisher<void>> m_plain;
    std::unique_ptr<imu_subscriber> m_demand;
};

}

#endif

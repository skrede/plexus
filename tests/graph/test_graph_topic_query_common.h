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

// A node's table holds what its PEERS declared, so one topic can only be seen to disagree with
// itself at a THIRD node: both rival publishers must be remote to whoever enumerates them.
struct trio : net
{
    plexus::node_id id_c{make_id(0x0C)};
    plexus::inproc::inproc_transport<> tc{h.ex, h.bus};
    inproc_node c{h.ex, h.disc, id_c, tc, opts_for(true)};

    // A publishes the topic as one type, C as another, and B demands it — so B's table carries two
    // publisher edges over one topic entry holding two distinct type names.
    void declare_all()
    {
        connect();
        c.listen({"inproc", "host-c:7000"});
        drive();
        REQUIRE(c.router().is_connected(id_b));

        a.router().messages().declare(k_imu_topic, plexus::topic_qos{}, k_imu_type_id, false, k_imu_type_name);
        c.router().messages().declare(k_imu_topic, plexus::topic_qos{}, k_rival_type_id, false, k_rival_type_name);
        c.router().messages().declare(k_plain_topic, plexus::topic_qos{});
        m_demand = std::make_unique<imu_subscriber>(b, k_imu_topic, [](const std::uint32_t &) {});
        drive();
    }

private:
    std::unique_ptr<imu_subscriber> m_demand;
};

}

#endif

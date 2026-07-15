// The node's own endpoints in its own table. Every case here reads a query at a node that is
// itself party to what it is asking about — the counts, the by-participant re-key, and the type
// disagreement a publisher is one half of — plus the retire path those edges leave by.

#include "test_graph_topic_query_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace graph_topics_fixture;

// A count is over the whole graph, the counting node included — the parity the RMW verb this
// mirrors has. The same two publishers must total two at every node, whichever of them is asking.
TEST_CASE("graph.self_edges: a node counts its own publishers and subscribers", "[graph]")
{
    trio n;
    n.declare_all();

    REQUIRE(n.a.count_publishers(k_imu_topic) == 2);
    REQUIRE(n.c.count_publishers(k_imu_topic) == 2);
    REQUIRE(n.c.count_publishers(k_plain_topic) == 1);
    REQUIRE(n.b.count_subscribers(k_imu_topic) == 1);
}

// The by-participant view keyed on the asking node's OWN id: the query a node runs to answer what
// it itself publishes, which reads empty unless its own edges are in its own table.
TEST_CASE("graph.self_edges: a node re-keys its own topics by its own id", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> buffer{};
    REQUIRE(n.a.topics_published_by(n.id_a, buffer).count == 1);
    REQUIRE(find_record(buffer, 1, k_imu_topic, n.id_a) != nullptr);

    const auto own_by_c = n.c.topics_published_by(n.id_c, buffer);
    REQUIRE(own_by_c.count == 2);
    REQUIRE(find_record(buffer, own_by_c.count, k_imu_topic, n.id_c) != nullptr);
    REQUIRE(find_record(buffer, own_by_c.count, k_plain_topic, n.id_c) != nullptr);

    REQUIRE(n.b.topics_subscribed_by(n.id_b, buffer).count == 1);
    REQUIRE(n.a.topics_published_by(n.id_b, buffer).count == 0);
}

// The conflict must be visible to the nodes IN it, not only to a bystander: a publisher that could
// not see its own declaration could not see what it disagrees with, which is the party that most
// needs to know.
TEST_CASE("graph.self_edges: a party to a type disagreement sees the disagreement", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> at_c{};
    const auto seen_by_c = n.c.topics(at_c, pattern(k_imu_topic));
    REQUIRE(seen_by_c.count == 3);
    for(std::size_t i = 0; i < seen_by_c.count; ++i)
        REQUIRE(names_of(at_c[i]) == sorted({k_imu_type_name, k_rival_type_name}));

    std::array<topic_record, 8> at_a{};
    const auto seen_by_a = n.a.topics(at_a, pattern(k_imu_topic));
    REQUIRE(seen_by_a.count == 3);
    REQUIRE(names_of(at_a[0]) == sorted({k_imu_type_name, k_rival_type_name}));
}

// A retired publisher stops being counted at its own node, and the rival publisher on the same
// topic is untouched — the removal is one edge, not a topic. The peers keep what they were told:
// the local edge leaves by the retire path, and no wire verb withdraws a declaration.
TEST_CASE("graph.self_edges: a retired local publisher stops counting itself", "[graph]")
{
    trio n;
    n.declare_all();
    REQUIRE(n.a.count_publishers(k_imu_topic) == 2);

    n.retire_imu_publisher();

    REQUIRE(n.a.count_publishers(k_imu_topic) == 1);
    std::array<topic_record, 8> buffer{};
    REQUIRE(n.a.topics_published_by(n.id_a, buffer).count == 0);
    REQUIRE(n.a.count_subscribers(k_imu_topic) == 1);

    const auto remaining = n.a.topics(buffer);
    REQUIRE(find_record(buffer, remaining.count, k_imu_topic, n.id_c) != nullptr);
}

// One participant is one edge however many handles it holds, so the count must not move until the
// last of them goes. Retiring on the first would un-publish a topic still being published.
TEST_CASE("graph.self_edges: a second publisher handle is the same edge, not a second one", "[graph]")
{
    trio n;
    n.declare_all();
    REQUIRE(n.a.count_publishers(k_imu_topic) == 2);

    {
        imu_publisher second{n.a, k_imu_topic, plexus::typed_publisher_options{}, imu_codec{}};
        n.drive();
        REQUIRE(n.a.count_publishers(k_imu_topic) == 2);
    }
    n.drive();

    REQUIRE(n.a.count_publishers(k_imu_topic) == 2);
    n.retire_imu_publisher();
    REQUIRE(n.a.count_publishers(k_imu_topic) == 1);
}

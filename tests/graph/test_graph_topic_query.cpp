// The public query surface over the propagated edges: the bounded enumeration and its wildcard
// filter, the count reductions, the by-participant re-key, and the disagreement two publishers of
// one topic leave visible.

#include "test_graph_topic_query_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace graph_topics_fixture;

TEST_CASE("graph.topic_query: the enumeration lists every propagated edge with its type names", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> buffer{};
    const auto snapshot = n.b.topics(buffer);
    REQUIRE_FALSE(snapshot.truncated);
    REQUIRE(snapshot.count == 3);

    const auto *imu = find_record(buffer, snapshot.count, k_imu_topic, n.id_a);
    REQUIRE(imu != nullptr);
    REQUIRE(imu->role == topic_role::publisher);
    REQUIRE(names_of(*imu) == sorted({k_imu_type_name, k_rival_type_name}));

    // A topic no participant typed enumerates all the same: an empty list is the undeclared state,
    // not an absent topic.
    const auto *plain = find_record(buffer, snapshot.count, k_plain_topic, n.id_c);
    REQUIRE(plain != nullptr);
    REQUIRE(names_of(*plain).empty());
}

TEST_CASE("graph.topic_query: an undersized buffer clips the enumeration and says so", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 2> buffer{};
    const auto snapshot = n.b.topics(buffer);
    REQUIRE(snapshot.count == 2);
    REQUIRE(snapshot.truncated);
}

TEST_CASE("graph.topic_query: a wildcard filter admits only the topics it intersects", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> buffer{};
    const auto sensors = n.b.topics(buffer, pattern("sensor/*"));
    REQUIRE(sensors.count == 2);
    for(std::size_t i = 0; i < sensors.count; ++i)
        REQUIRE(buffer[i].name == k_imu_topic);

    // An absent filter is not an empty one: it admits the topics the pattern excluded.
    REQUIRE(n.b.topics(buffer).count == 3);
    REQUIRE(n.b.topics(buffer, pattern("nothing/here")).count == 0);
}

// The reduction is checked against a topology whose edges are counted by hand: two publishers and
// one subscriber of one topic, each a distinct participant. A publisher landing under a session's
// provisional id rather than its proven one would read as four.
TEST_CASE("graph.topic_query: the counts reduce over the edges a known topology declares", "[graph]")
{
    trio n;
    n.declare_all();

    REQUIRE(n.b.count_publishers(k_imu_topic) == 2);
    REQUIRE(n.b.count_publishers(k_plain_topic) == 1);
    REQUIRE(n.b.count_publishers("no/such/topic") == 0);

    // A node's table holds what its PEERS declared: B's own subscription is local and folds
    // nowhere, while A and C each learned of it over the wire.
    REQUIRE(n.b.count_subscribers(k_imu_topic) == 0);
    REQUIRE(n.a.count_subscribers(k_imu_topic) == 1);
    REQUIRE(n.c.count_subscribers(k_imu_topic) == 1);
}

TEST_CASE("graph.topic_query: the by-participant re-key splits publisher from subscriber", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> buffer{};
    const auto published = n.b.topics_published_by(n.id_c, buffer);
    REQUIRE(published.count == 2);
    REQUIRE(find_record(buffer, published.count, k_imu_topic, n.id_c) != nullptr);
    REQUIRE(find_record(buffer, published.count, k_plain_topic, n.id_c) != nullptr);

    REQUIRE(n.b.topics_published_by(n.id_a, buffer).count == 1);
    REQUIRE(n.b.topics_subscribed_by(n.id_c, buffer).count == 0);
    REQUIRE(n.a.topics_subscribed_by(n.id_b, buffer).count == 1);
}

// Two publishers of one topic disagreeing on its type is never settled by a silent first-wins: the
// enumeration hands the consumer both names and the count above one that says they disagree.
TEST_CASE("graph.topic_query: two publishers disagreeing on a topic's type stay visible as a list", "[graph]")
{
    trio n;
    n.declare_all();

    std::array<topic_record, 8> buffer{};
    const auto snapshot = n.b.topics(buffer, pattern(k_imu_topic));
    REQUIRE(snapshot.count == 2);
    for(std::size_t i = 0; i < snapshot.count; ++i)
    {
        REQUIRE(buffer[i].types.count == 2);
        REQUIRE(names_of(buffer[i]) == sorted({k_imu_type_name, k_rival_type_name}));
    }
    REQUIRE(n.b.topic_truncations() == 0);
    REQUIRE(n.b.topics_dropped() == 0);

    // Only a third party sees both sides: a table holds what its PEERS declared, so C — a party to
    // the very disagreement — enumerates its rival's type and not its own.
    std::array<topic_record, 8> at_c{};
    const auto seen_by_c = n.c.topics(at_c, pattern(k_imu_topic));
    REQUIRE(seen_by_c.count == 2);
    REQUIRE(names_of(at_c[0]) == std::vector<std::string>{k_imu_type_name});
}


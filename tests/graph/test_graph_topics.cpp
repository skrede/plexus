// The cross-participant propagation oracle: a publisher's declaration and a subscriber's
// type_name reach the peer's owning topic table over the authenticated session, survive a
// reconnect through the replay, and keep the three declaration states apart end-to-end.

#include "test_graph_topics_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace graph_topics_fixture;

TEST_CASE("graph.topics: a declaration and a subscriber's type_name reach the peer's table", "[graph]")
{
    net n;
    n.connect();

    imu_publisher pub{n.a, k_imu_topic, {}, imu_codec{}};
    imu_subscriber sub{n.b, k_imu_topic, [](const std::uint32_t &) {}};
    n.drive();

    const auto on_b = edges_of(n.b);
    const auto *declared = find_edge(on_b, k_imu_topic, topic_role::publisher);
    REQUIRE(declared != nullptr);
    REQUIRE(declared->types == std::vector<std::string>{k_imu_type_name});

    const auto on_a = edges_of(n.a);
    const auto *demanded = find_edge(on_a, k_imu_topic, topic_role::subscriber);
    REQUIRE(demanded != nullptr);
    REQUIRE(demanded->types == std::vector<std::string>{k_imu_type_name});
}

// A pair that both dials and accepts holds two sessions for one peer. The edges must still name
// the one participant the awareness table names — not a slot's provisional id, and not twice.
TEST_CASE("graph.topics: an edge names the proven participant exactly once", "[graph]")
{
    net n;
    n.connect();

    imu_publisher pub{n.a, k_imu_topic, {}, imu_codec{}};
    n.drive();

    const auto on_b = edges_of(n.b);
    REQUIRE(on_b.size() == 1);
    REQUIRE(on_b.front().node == n.id_a);
    REQUIRE(holds_id(n.b, n.id_a));
}

// The declaration is asserted with no session to carry it, so the only path to B's table is the
// replay a completing session runs — the same seam a reconnect drives.
TEST_CASE("graph.topics: a session completing replays declarations made before it existed", "[graph]")
{
    net n;

    imu_publisher pub{n.a, k_imu_topic, {}, imu_codec{}};
    n.drive();
    REQUIRE(edges_of(n.b).empty());

    n.connect();
    REQUIRE(find_edge(edges_of(n.b), k_imu_topic, topic_role::publisher) != nullptr);
}

TEST_CASE("graph.topics: a torn session takes its peer's records with it and a reconnect restores them", "[graph]")
{
    net n;
    n.connect();

    imu_publisher pub{n.a, k_imu_topic, {}, imu_codec{}};
    n.drive();
    REQUIRE(find_edge(edges_of(n.b), k_imu_topic, topic_role::publisher) != nullptr);

    n.tear_down();
    REQUIRE(edges_of(n.b).empty());

    n.reconnect();
    REQUIRE(find_edge(edges_of(n.b), k_imu_topic, topic_role::publisher) != nullptr);
}

TEST_CASE("graph.topics: the three declaration states survive the round trip apart", "[graph]")
{
    net n;
    n.connect();

    imu_publisher typed{n.a, k_imu_topic, {}, imu_codec{}};
    n.a.router().messages().declare(k_plain_topic, plexus::topic_qos{});
    n.a.router().messages().declare(k_unnamed_topic, plexus::topic_qos{}, k_unnamed_type_id);
    n.drive();

    const auto on_b = edges_of(n.b);

    const auto *declared = find_edge(on_b, k_imu_topic, topic_role::publisher);
    REQUIRE(declared != nullptr);
    REQUIRE(declared->types == std::vector<std::string>{k_imu_type_name});

    // No type asserted at all: the list is empty, not a list holding one empty name.
    const auto *undeclared = find_edge(on_b, k_plain_topic, topic_role::publisher);
    REQUIRE(undeclared != nullptr);
    REQUIRE(undeclared->types.empty());

    // A type asserted without a name: one entry, and that entry is the empty name.
    const auto *unnamed = find_edge(on_b, k_unnamed_topic, topic_role::publisher);
    REQUIRE(unnamed != nullptr);
    REQUIRE(unnamed->types == std::vector<std::string>{""});
}

#include "test_bytes_pubsub_common.h"

using namespace bytes_pubsub_fixture;

TEST_CASE("bytes pub/sub: the 2-arg path is unchanged when a source-identity topic publishes", "[node][pubsub]")
{
    net n;
    n.connect();

    std::vector<std::string> got;
    inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
    inproc_publisher p{n.b, "topic", plexus::topic_qos{}, /*emit_source_identity=*/true};
    n.drive();

    p.publish(as_bytes("bytes-only"));
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == "bytes-only");
}

TEST_CASE("bytes pub/sub: the standing fan reaches a peer discovered AFTER the subscriber "
          "registers, looped",
          "[node][pubsub]")
{
    constexpr int k_iterations = 5;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        net n;

        // A subscribes with NO peer known yet (B has not been constructed-as-connected;
        // only A has listened). The demand is standing.
        std::vector<std::string> got;
        inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        n.a.listen({"inproc", "host-a:5000"});
        n.drive();

        // Now B comes up, listens, and is discovered. A's standing demand re-fans to B
        // with no further user action; B's publisher then reaches A's callback.
        n.b.listen({"inproc", "host-b:6000"});
        n.drive();
        REQUIRE(n.a.router().is_connected(n.id_b));

        inproc_publisher p{n.b, "topic"};
        n.drive();
        p.publish(as_bytes("late-join"));
        n.drive();

        REQUIRE(got.size() == 1);
        REQUIRE(got.front() == "late-join");
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

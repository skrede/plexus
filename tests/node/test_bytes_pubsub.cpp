#include "test_bytes_pubsub_common.h"

using namespace bytes_pubsub_fixture;

TEST_CASE("bytes pub/sub: end-to-end delivery is byte-identical, looped", "[node][pubsub]")
{
    constexpr int k_iterations = 8;
    net n;
    n.connect();

    std::vector<std::string> got;
    inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
    inproc_publisher p{n.b, "topic"};
    n.drive();

    int delivered = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const std::string payload = "iteration-" + std::to_string(i);
        p.publish(as_bytes(payload));
        n.drive();
        REQUIRE(got.size() == static_cast<std::size_t>(i + 1));
        REQUIRE(got.back() == payload);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("bytes pub/sub: the 3-arg callback receives a populated message_info with the source gid", "[node][pubsub]")
{
    net n;
    n.connect();

    std::vector<std::string> got;
    std::vector<message_info> infos;
    inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b, const message_info &mi)
                        {
                            got.push_back(to_string(b));
                            infos.push_back(mi);
                        }};
    // A source-identity-emitting publisher: its gid rides the frame and reaches the info.
    inproc_publisher p{n.b, "topic", plexus::topic_qos{}, /*emit_source_identity=*/true};
    n.drive();

    p.publish(as_bytes("with-info"));
    n.drive();

    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == "with-info");
    REQUIRE(infos.size() == 1);
    REQUIRE(infos.front().reception_timestamp != 0);
    REQUIRE(infos.front().source_identity.has_value());
    REQUIRE(infos.front().source_identity->node_id() == n.id_b);
}

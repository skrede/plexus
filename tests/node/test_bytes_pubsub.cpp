// End-to-end bytes pub/sub over the node facade, two nodes on a shared inproc bus +
// static_discovery (eager-dialing). It proves:
//   - byte-identical delivery from a publisher.publish to a subscriber callback, looped;
//   - the 3-arg callback receives a populated message_info while the 2-arg path is
//     unchanged, and a source-identity-emitting publisher's gid reaches the 3-arg info;
//   - THE STANDING FAN (D-01): a subscriber registered BEFORE the publishing peer is
//     known still receives the peer's later publishes with no further user action.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_publisher = plexus::publisher<inproc_policy>;
using inproc_subscriber = plexus::subscriber<inproc_policy>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Only the SUBSCRIBER node (A) dials eagerly; the publisher node (B) stays lazy and
// accepts. Both nodes eager over a shared bus would mutually dial into TWO sessions per
// peer (simultaneous connect), double-delivering each publish — single-dialer keeps one.
plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive() { ex.drain(); }

    // Both nodes listen and discover each other; A's eager dial converges the single
    // connection (B is lazy — it accepts A's dial as an inbound session, which is not
    // keyed under A's id the way is_connected's dialed-slot check reads, so only the
    // dialer's direction is asserted here; delivery over the inbound channel is the
    // real proof).
    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

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
    inproc_subscriber s{n.a, "topic",
                        [&](std::span<const std::byte> b, const message_info &mi) {
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

TEST_CASE("bytes pub/sub: the standing fan reaches a peer discovered AFTER the subscriber registers, looped", "[node][pubsub]")
{
    constexpr int k_iterations = 5;
    int proven = 0;
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

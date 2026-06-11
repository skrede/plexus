// The pub/sub handle-semantics oracle, over the deterministic inproc backend. It proves
// the move-only RAII contract and the ctor-canonical registration/retire of the
// publisher and subscriber handles:
//   - move transfers the endpoint; the moved-from handle's destructor is a no-op;
//     a double-move is safe;
//   - dropping a subscriber retires its demand: a later publish does NOT reach its
//     callback, and dropping the LAST local subscriber for an fqn removes the engine's
//     remembered demand toward the peer;
//   - two subscribers on one fqn both fire and retire independently;
//   - a publisher is move-only and non-copyable (the type-trait contract).
// Two nodes over a shared inproc bus + static_discovery, eager-dialing so awareness
// converges to a live connection on a single drain.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/node_name.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <type_traits>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_publisher = plexus::publisher<inproc_policy>;
using inproc_subscriber = plexus::subscriber<inproc_policy>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Only the SUBSCRIBER node dials (eager); the publisher node stays lazy and merely
// accepts. With both nodes eager over a shared bus they would mutually dial and form
// TWO sessions per peer (the simultaneous-connect property), double-delivering every
// publish — a single-dialer topology keeps exactly one connection, the realistic shape.
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

// A two-node net: A subscribes, B publishes. Both listen and discover each other over a
// shared static_discovery, eager-dialing to a live connection.
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

    net()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
        REQUIRE(a.router().is_connected(id_b));
    }

    void drive() { ex.drain(); }

    std::size_t a_demand_for(const std::string &fqn)
    {
        std::size_t count = 0;
        for(const auto &d : a.router().messages().remembered_topics(plexus::io::node_name_of(id_b)))
            if(d.fqn == fqn)
                ++count;
        return count;
    }
};

}

TEST_CASE("handles: a publisher is move-only", "[node][handles]")
{
    static_assert(!std::is_copy_constructible_v<inproc_publisher>);
    static_assert(!std::is_copy_assignable_v<inproc_publisher>);
    static_assert(std::is_nothrow_move_constructible_v<inproc_publisher>);
    static_assert(std::is_nothrow_move_assignable_v<inproc_publisher>);
}

TEST_CASE("handles: a subscriber is move-only", "[node][handles]")
{
    static_assert(!std::is_copy_constructible_v<inproc_subscriber>);
    static_assert(!std::is_copy_assignable_v<inproc_subscriber>);
    static_assert(std::is_nothrow_move_constructible_v<inproc_subscriber>);
    static_assert(std::is_nothrow_move_assignable_v<inproc_subscriber>);
}

TEST_CASE("handles: moving a subscriber transfers the demand; the moved-from dtor is a no-op", "[node][handles]")
{
    net n;
    std::vector<std::string> got;

    {
        inproc_subscriber s1{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        n.drive();

        // Move the live demand into s2, then double-move into s3. The moved-from s1/s2
        // dtors must NOT retire the demand — only s3's does.
        inproc_subscriber s2{std::move(s1)};
        inproc_subscriber s3{std::move(s2)};

        inproc_publisher p{n.b, "topic"};
        n.drive();
        p.publish(as_bytes("payload"));
        n.drive();

        REQUIRE(got.size() == 1);
        REQUIRE(got.front() == "payload");
    }
    // s3 went out of scope: the demand is retired (last subscriber for the fqn gone).
    REQUIRE(n.a_demand_for("topic") == 0);
}

TEST_CASE("handles: dropping a subscriber stops its callback and retires the engine demand", "[node][handles]")
{
    net n;
    std::vector<std::string> got;
    inproc_publisher p{n.b, "topic"};

    {
        inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        n.drive();
        REQUIRE(n.a_demand_for("topic") == 1);

        p.publish(as_bytes("first"));
        n.drive();
        REQUIRE(got.size() == 1);
    }

    // The subscriber is gone: its demand is retired and a further publish never fires it.
    REQUIRE(n.a_demand_for("topic") == 0);
    p.publish(as_bytes("after-drop"));
    n.drive();
    REQUIRE(got.size() == 1);   // still 1 — no callback after drop
}

TEST_CASE("handles: two subscribers on one fqn both fire and retire independently", "[node][handles]")
{
    net n;
    std::vector<std::string> got1;
    std::vector<std::string> got2;
    inproc_publisher p{n.b, "topic"};

    auto s1 = inproc_subscriber{n.a, "topic", [&](std::span<const std::byte> b) { got1.push_back(to_string(b)); }};
    {
        inproc_subscriber s2{n.a, "topic", [&](std::span<const std::byte> b) { got2.push_back(to_string(b)); }};
        n.drive();

        p.publish(as_bytes("both"));
        n.drive();
        REQUIRE(got1.size() == 1);
        REQUIRE(got2.size() == 1);

        // The engine demand is still live while ANY local subscriber holds the fqn.
        REQUIRE(n.a_demand_for("topic") == 1);
    }
    // s2 retired, but s1 keeps the fqn alive: the demand persists and s1 still fires.
    REQUIRE(n.a_demand_for("topic") == 1);
    p.publish(as_bytes("only-s1"));
    n.drive();
    REQUIRE(got1.size() == 2);
    REQUIRE(got2.size() == 1);
}

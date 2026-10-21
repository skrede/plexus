#include "test_node_close_common.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using namespace node_close_fixture;

namespace {

template<typename Node>
void hang_up_or_fail(Node &node, const endpoint &ep)
{
    if constexpr(requires { node.hang_up(ep); })
        node.hang_up(ep);
    else
        FAIL("the node has no hang_up()");
}

}

TEST_CASE("hang_up a hung-up endpoint's session drops at its peer and is never redialed", "[node][hang_up][!shouldfail]")
{
    close_net net;
    net.connect();
    const std::uint32_t attempts = net.a.router().attempt_count(net.dialed_slot);

    hang_up_or_fail(net.a, net.ep_b);
    net.ex.drain();
    REQUIRE(net.b_sessions() == 0);
    REQUIRE_FALSE(net.a.router().has_session(net.dialed_slot));
    REQUIRE(net.live_channels() == 1);

    const std::uint64_t before = net.mint_key();
    net.run_for(k_backoff * 20);
    REQUIRE(net.mint_key() == before + 1);
    REQUIRE(net.a.router().attempt_count(net.dialed_slot) == attempts);
}

TEST_CASE("hang_up a dial after a hang-up dials again and the session comes back", "[node][hang_up][!shouldfail]")
{
    close_net net;
    net.connect();

    hang_up_or_fail(net.a, net.ep_b);
    net.ex.drain();
    REQUIRE(net.b_sessions() == 0);

    net.a.dial(net.ep_b);
    net.ex.drain();
    REQUIRE(net.a.router().is_connected(net.dialed_slot));
    REQUIRE(net.b_sessions() == 1);
}

TEST_CASE("hang_up of an endpoint the node never dialed leaves the session accepted from that peer", "[node][hang_up][!shouldfail]")
{
    close_net net;
    net.connect();

    hang_up_or_fail(net.b, net.ep_a);
    net.ex.drain();
    REQUIRE(net.b_sessions() == 1);
    REQUIRE(net.a.router().is_connected(net.dialed_slot));
}

TEST_CASE("hang_up of a dial still in flight retires its late channel and builds no session", "[node][hang_up][!shouldfail]")
{
    close_net net;
    net.a.dial(net.ep_b);

    hang_up_or_fail(net.a, net.ep_b);
    net.ex.drain();
    REQUIRE_FALSE(net.a.router().has_session(net.dialed_slot));
    REQUIRE(net.b_sessions() == 0);
    REQUIRE(net.live_channels() == 1);
}

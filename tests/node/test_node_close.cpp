#include "test_node_close_common.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using namespace node_close_fixture;

TEST_CASE("node_close a closed node's sessions drop at its peer and its slots hold no channel", "[node][node_close][!shouldfail]")
{
    close_net net;
    net.connect();
    REQUIRE(net.live_channels() == 2);

    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(net.b_sessions() == 0);
    REQUIRE_FALSE(net.a.router().has_session(net.dialed_slot));
    REQUIRE(net.live_channels() == 1);
}

TEST_CASE("node_close a closed node arms no redial however long its executor runs", "[node][node_close][!shouldfail]")
{
    close_net net;
    net.connect();
    const std::uint32_t attempts = net.a.router().attempt_count(net.dialed_slot);

    close_or_fail(net.a);
    const std::uint64_t before = net.mint_key();
    net.run_for(k_backoff * 20);
    REQUIRE(net.mint_key() == before + 1);
    REQUIRE(net.a.router().attempt_count(net.dialed_slot) == attempts);
    REQUIRE(net.b_sessions() == 0);
}

TEST_CASE("node_close close is idempotent and stops the node's discovery once", "[node][node_close][!shouldfail]")
{
    close_net net;
    net.connect();

    close_or_fail(net.a);
    close_or_fail(net.a);
    net.ex.drain();
    REQUIRE(net.disc_a.stops() == 1);
    REQUIRE(net.disc_b.stops() == 0);
}

TEST_CASE("node_close a closed composed node leaves no callback of its own on its member transports", "[node][node_close][!shouldfail]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    plexus::io::intra_node_transport<inproc_policy> self;
    recording_member member;
    counting_discovery disc;
    composed_node node{ex, disc, make_id(0x0C), self, member, make_opts()};
    ex.drain();
    REQUIRE(member.any_installed());

    close_or_fail(node);
    ex.drain();
    REQUIRE_FALSE(member.any_installed());
}

TEST_CASE("node_close a never-closed node destroyed with its executor not run again stays clean", "[node][node_close]")
{
    close_net net;
    net.connect();
    REQUIRE(net.disc_a.stops() == 0);
}

// Two inproc engines that dial each other end with two sessions for one proven identity. Both ends pick the same
// survivor by id, so each keeps exactly one complete session per peer, in either completion order or when both
// dial at once.

#include "test_crossed_dial_common.h"

#include "plexus/io/endpoint_id.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>

using namespace crossed_dial_fixture;

namespace {

constexpr crossing k_orders[] = {crossing::at_once, crossing::newcomer_loses, crossing::incumbent_loses};

node_id dialed_by_a(const crossed_net &net, crossing order)
{
    return order == crossing::incumbent_loses ? endpoint_id(net.ep_b) : net.id_b;
}

bool fired_nothing(const recording_observer::counts &c)
{
    return c.connected == 0 && c.ready == 0 && c.disconnected == 0 && c.reconnected == 0 && c.dead == 0 && c.rejected == 0;
}

}

TEST_CASE("crossed dial: each node keeps exactly one session for its peer", "[integration][crossed_dial]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);

        CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
        CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
    }
}

TEST_CASE("crossed dial: the higher id keeps the session it dialed and the lower id the one it accepted", "[integration][crossed_dial]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);

        CHECK(kept_slot(net.a.e, net.id_b) == std::optional<node_id>{dialed_by_a(net, order)});
        CHECK(kept_slot(net.b.e, net.id_a) == std::optional<node_id>{inbound_slot(1)});
    }
}

TEST_CASE("crossed dial: a session that loses on completion fires no lifecycle edge", "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net;
    net.cross(crossing::newcomer_loses);

    CHECK(fired_nothing(net.a.edges.for_peer(inbound_slot(1))));
    CHECK(fired_nothing(net.b.edges.for_peer(endpoint_id(net.ep_a))));
    CHECK(net.a.edges.for_peer(net.id_b).connected == 1);
    CHECK(net.b.edges.for_peer(inbound_slot(1)).connected == 1);
    CHECK(net.b.e.registry().driver_for(endpoint_id(net.ep_a)).redial_denied());
}

TEST_CASE("crossed dial: an incumbent that loses closes without a disconnected edge and its demand moves to the survivor", "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net;
    net.cross(crossing::incumbent_loses);

    CHECK(!net.b.e.is_connected(net.id_a));
    CHECK(!net.a.e.is_connected(inbound_slot(1)));
    CHECK(net.b.edges.for_peer(net.id_a).disconnected == 0);
    CHECK(net.a.edges.for_peer(inbound_slot(1)).disconnected == 0);
    CHECK(net.b.e.registry().driver_for(net.id_a).redial_denied());

    auto *survivor = net.b.e.session_for(inbound_slot(1));
    REQUIRE(survivor != nullptr);
    REQUIRE(survivor->is_complete());
    const auto &demands = net.b.e.messages().remembered_topics(survivor->msg_peer().node_name);
    CHECK(demands.size() == 1);
    net.publish_from(net.a);
    CHECK(net.b.received.size() == 1);
}

TEST_CASE("crossed dial: each resolution logs one warning naming the peer and the kept session", "[integration][crossed_dial]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);

        const auto on_a = resolution_lines(net.a.log);
        const auto on_b = resolution_lines(net.b.log);
        REQUIRE(on_a.size() == 1);
        REQUIRE(on_b.size() == 1);
        CHECK(on_a.front() == "plexus: crossed dial with " + hex_of(net.id_b) + ", kept the dialed session");
        CHECK(on_b.front() == "plexus: crossed dial with " + hex_of(net.id_a) + ", kept the accepted session");
    }
}

TEST_CASE("crossed dial: sessions of two different peers are never merged", "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net;
    net.a.e.note_peer(net.id_b, net.ep_b);
    net.a.e.reach(net.id_b);
    net.c.e.dial(net.ep_a);
    net.drive();

    CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
    CHECK(complete_sessions_for(net.a.e, net.id_c) == 1);
    CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
    CHECK(complete_sessions_for(net.c.e, net.id_a) == 1);
    CHECK(resolution_lines(net.a.log).empty());
}

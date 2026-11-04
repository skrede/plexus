#include "test_crossed_dial_common.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace crossed_dial_fixture;

namespace {

constexpr crossing k_orders[] = {crossing::at_once, crossing::newcomer_loses, crossing::incumbent_loses};
constexpr auto k_past_window  = k_window + 2 * k_step;

node_side &subscriber_of(crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? net.a : net.b;
}

node_side &publisher_of(crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? net.b : net.a;
}

const node_id &publisher_id(const crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? net.id_b : net.id_a;
}

bool reports_only(const node_side &side, const node_id &id)
{
    const auto named = [&id](const auto &ev) { return ev.id == id; };
    return side.edges.recorded_peers() == 1 && side.edges.for_peer(id).connected >= 1 && std::ranges::all_of(side.verdicts.events, named);
}

bool last_alive(const recording_liveliness_observer &obs, const node_id &id)
{
    for(auto it = obs.events.rbegin(); it != obs.events.rend(); ++it)
        if(it->id == id)
            return it->verdict == liveliness_verdict::alive;
    return false;
}

void reach_again(crossed_net &net, node_side &side, const node_id &peer, const endpoint &ep)
{
    side.e.note_peer(peer, ep);
    side.e.reach(peer);
    net.drive();
}

}

// Each accepted end receives the other's request before the response to its own, so b's accepted session completes
// while b's dial, filed under a's id, is still handshaking.
TEST_CASE("proven identity: an accepted session completing while the crossed dial is still handshaking reports only under the proven id", "[integration][proven_identity]")
{
    manual_clock::reset();
    crossed_net net;
    net.a.e.note_peer(net.id_b, net.ep_b);
    net.b.e.note_peer(net.id_a, net.ep_a);
    net.a.e.reach(net.id_b);
    net.b.e.reach(net.id_a);
    while(complete_sessions_for(net.b.e, net.id_a) == 0 && net.ex.step())
    {
    }
    REQUIRE(complete_sessions_for(net.b.e, net.id_a) == 1);
    REQUIRE(net.b.e.has_session(net.id_a));
    REQUIRE(!net.b.e.is_connected(net.id_a));

    net.drive();
    net.advance_by(k_past_window);
    CHECK(resolution_lines(net.b.log).size() == 1);
    CHECK(reports_only(net.b, net.id_a));
    CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
    CHECK(net.b.e.is_connected(net.id_a));
}

TEST_CASE("proven identity: a crossed pair's loser leaves service and its slot is erased once drained", "[integration][proven_identity]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        const std::size_t slots_a = net.a.e.registry().slot_count();
        const std::size_t slots_b = net.b.e.registry().slot_count();
        if(order == crossing::at_once)
            net.b.e.subscribe(net.id_a, k_topic);
        net.cross(order);
        net.advance_by(k_past_window);
        CHECK(net.a.e.registry().slot_count() == slots_a + 1);
        CHECK(net.b.e.registry().slot_count() == slots_b + 1);

        const std::uint32_t dialing_a = net.a.e.attempt_count(net.id_b);
        const std::uint32_t dialing_b = net.b.e.attempt_count(net.id_a);
        reach_again(net, net.a, net.id_b, net.ep_b);
        reach_again(net, net.b, net.id_a, net.ep_a);
        CHECK(net.a.e.registry().slot_count() == slots_a + 1);
        CHECK(net.b.e.registry().slot_count() == slots_b + 1);
        CHECK(net.a.e.attempt_count(net.id_b) == dialing_a);
        CHECK(net.b.e.attempt_count(net.id_a) == dialing_b);
        CHECK(resolution_lines(net.a.log).size() == 1);
        CHECK(resolution_lines(net.b.log).size() == 1);
        net.publish_from(publisher_of(net, order));
        CHECK(subscriber_of(net, order).received.size() == 1);
    }
}

TEST_CASE("proven identity: a crossed survivor keeps the demand and liveliness made before the cross", "[integration][proven_identity]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net{opts(combine::any_signal_alive)};
        if(order == crossing::at_once)
            net.b.e.subscribe(net.id_a, k_topic);
        net.cross(order);
        net.advance_by(std::chrono::nanoseconds(k_ttl) + 2 * k_step);
        net.advance_by(k_past_window);

        node_side &subscriber = subscriber_of(net, order);
        CHECK(last_alive(subscriber.verdicts, publisher_id(net, order)));
        CHECK(complete_sessions_for(subscriber.e, publisher_id(net, order)) == 1);
        net.publish_from(publisher_of(net, order));
        CHECK(subscriber.received.size() == 1);
    }
}

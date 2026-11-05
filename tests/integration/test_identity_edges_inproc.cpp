#include "test_identity_edges_common.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <vector>
#include <cstdint>

using namespace identity_edges_fixture;

namespace {

constexpr crossing k_crossings[] = {crossing::at_once, crossing::newcomer_loses, crossing::incumbent_loses};
constexpr auto k_past_window     = k_window + 2 * k_step;
constexpr auto k_redial          = std::chrono::seconds(1);
constexpr std::int32_t k_cycles  = 20;

node_id kept_on_a(const crossed_net &net, crossing order)
{
    return order == crossing::incumbent_loses ? endpoint_id(net.ep_b) : net.id_b;
}

bool last_alive(const recording_liveliness_observer &obs, const node_id &id)
{
    for(auto it = obs.events.rbegin(); it != obs.events.rend(); ++it)
        if(it->id == id)
            return it->verdict == liveliness_verdict::alive;
    return false;
}

}

TEST_CASE("identity edges: each end of a crossed pair reports its kept identity up once, in every completion order", "[integration][identity_edges]")
{
    for(const crossing order : k_crossings)
    {
        INFO("order " << static_cast<std::int32_t>(order));
        manual_clock::reset();
        identity_log log_a;
        identity_log log_b;
        crossed_net net;
        net.a.e.add_observer(log_a);
        net.b.e.add_observer(log_b);
        net.cross(order);
        net.advance_by(k_past_window);

        const node_id kept_a = kept_on_a(net, order);
        CHECK(balanced(log_a, net.a.e));
        CHECK(balanced(log_b, net.b.e));
        CHECK(ups(log_a.of(kept_a)) == 1);
        CHECK(downs(log_a.of(kept_a)) == 0);
        CHECK(ups(log_b.of(net.id_a)) == 1);
        CHECK(downs(log_b.of(net.id_a)) == 0);
        CHECK(last_alive(net.a.verdicts, kept_a));
        CHECK(last_alive(net.b.verdicts, net.id_a));
    }
}

TEST_CASE("identity edges: a restarted peer's second accepted session replaces the first with no edge for the identity", "[integration][identity_edges]")
{
    manual_clock::reset();
    identity_log log_b;
    crossed_net net;
    net.b.e.add_observer(log_b);
    const node_id restarted = make_id(k_restarted);
    incarnation first{net};
    first.e.dial(net.ep_b);
    net.drive();
    incarnation second{net};
    second.e.dial(net.ep_b);
    net.drive();

    net.advance_by(k_past_window);
    REQUIRE(net.b.e.is_connected(restarted));
    CHECK(ups(log_b.of(restarted)) == 1);
    CHECK(downs(log_b.of(restarted)) == 0);
    CHECK(balanced(log_b, net.b.e));
}

TEST_CASE("identity edges: a dial to a second endpoint that replaces a card's session reports the card's id down and the endpoint's id up", "[integration][identity_edges]")
{
    manual_clock::reset();
    identity_log log_a;
    identity_log log_b;
    crossed_net net;
    net.a.e.add_observer(log_a);
    net.b.e.add_observer(log_b);
    const endpoint ep_b2{"inproc", "crossed-b-second"};
    net.b.e.listen(ep_b2);
    net.a.e.note_peer(net.id_b, net.ep_b);
    net.a.e.subscribe(net.id_b, k_topic);
    net.drive();
    REQUIRE(net.a.e.is_connected(net.id_b));

    net.a.e.dial(ep_b2);
    net.drive();
    net.advance_by(k_past_window);
    CHECK(ups(log_a.of(net.id_b)) == 1);
    CHECK(downs(log_a.of(net.id_b)) == 1);
    CHECK(ups(log_a.of(endpoint_id(ep_b2))) == 1);
    CHECK(downs(log_a.of(endpoint_id(ep_b2))) == 0);
    CHECK(balanced(log_a, net.a.e));
    CHECK(balanced(log_b, net.b.e));
}

TEST_CASE("identity edges: a crossed survivor's teardown reports its identity disconnected once", "[integration][identity_edges]")
{
    manual_clock::reset();
    identity_log log_b;
    crossed_net net;
    net.b.e.add_observer(log_b);
    net.cross(crossing::incumbent_loses);
    net.advance_by(k_past_window);
    REQUIRE(net.b.e.session_for(net.id_a) != nullptr);

    net.b.e.session_for(net.id_a)->tear_down();
    net.drive();
    CHECK(log_b.of(net.id_a) == std::vector<edge>{edge::connected, edge::ready, edge::disconnected});
    CHECK(balanced(log_b, net.b.e));
}

TEST_CASE("identity edges: repeated connect and disconnect cycles keep every identity's edges alternating", "[integration][identity_edges]")
{
    manual_clock::reset();
    identity_log log_a;
    identity_log log_b;
    crossed_net net;
    net.a.e.add_observer(log_a);
    net.b.e.add_observer(log_b);
    net.a.e.note_peer(net.id_b, net.ep_b);
    net.a.e.reach(net.id_b);
    net.drive();

    for(std::int32_t i = 0; i < k_cycles; ++i)
    {
        REQUIRE(net.b.e.session_for(net.id_a) != nullptr);
        net.b.e.session_for(net.id_a)->tear_down();
        net.advance_by(k_redial);
    }
    // A dropped dialed session ends at its redial, in the turn the redial completes, so the dialer ends up.
    CHECK(ups(log_a.of(net.id_b)) == k_cycles + 1);
    CHECK(downs(log_a.of(net.id_b)) == k_cycles);
    CHECK(balanced(log_a, net.a.e));
    CHECK(balanced(log_b, net.b.e));
}

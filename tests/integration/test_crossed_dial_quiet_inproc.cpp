// The loser of a crossed dial stays open for one handshake window, carrying nothing, and then closes without an
// edge. Neither the arbiter nor the monitor reports anything for it afterwards.

#include "test_crossed_dial_common.h"

#include "plexus/io/endpoint_id.h"
#include "plexus/io/subscriber_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>

using namespace crossed_dial_fixture;

namespace {

constexpr crossing k_orders[]  = {crossing::newcomer_loses, crossing::incumbent_loses};
constexpr combine k_policies[] = {combine::any_signal_alive, combine::all_required, combine::session_authoritative};
constexpr auto k_lease         = std::chrono::milliseconds(800);
constexpr auto k_past_miss     = std::chrono::milliseconds(600);
constexpr auto k_past_window   = k_window + k_step;

struct roles
{
    node_id loser_on_a;
    node_id loser_on_b;
    node_id kept_on_a;
    node_id kept_on_b;
    node_id dialed_on_a;
    node_id dialed_on_b;
};

roles roles_of(const crossed_net &net, crossing order)
{
    if(order == crossing::newcomer_loses)
        return {inbound_slot(1), endpoint_id(net.ep_a), net.id_b, inbound_slot(1), net.id_b, endpoint_id(net.ep_a)};
    return {inbound_slot(1), net.id_a, endpoint_id(net.ep_b), inbound_slot(1), endpoint_id(net.ep_b), net.id_a};
}

node_side &publisher_of(crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? net.b : net.a;
}

node_side &subscriber_of(crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? net.a : net.b;
}

bool open_and_idle(engine &e, const node_id &loser)
{
    auto *s = e.session_for(loser);
    return s != nullptr && !s->is_complete() && !s->pending().closing();
}

bool closed(engine &e, const node_id &loser)
{
    auto *s = e.session_for(loser);
    return s != nullptr && s->pending().closing();
}

bool no_teardown_edge(const recording_observer::counts &c)
{
    return c.disconnected == 0 && c.dead == 0 && c.reconnected == 0 && c.rejected == 0 && c.connected <= 1;
}

std::size_t liveness_for(const node_side &side, const node_id &id)
{
    std::size_t n = 0;
    for(const auto &ev : side.liveness)
        n += ev.endpoint == id ? 1 : 0;
    return n;
}

std::int32_t lost_for(const recording_liveliness_observer &obs, const node_id &id)
{
    std::int32_t n = 0;
    for(const auto &ev : obs.events)
        n += ev.id == id && ev.verdict == liveliness_verdict::lost ? 1 : 0;
    return n;
}

bool last_alive(const recording_liveliness_observer &obs, const node_id &id)
{
    for(auto it = obs.events.rbegin(); it != obs.events.rend(); ++it)
        if(it->id == id)
            return it->verdict == liveliness_verdict::alive;
    return false;
}

}

TEST_CASE("crossed dial: a losing session carries nothing for one handshake window and then closes quietly", "[integration][crossed_dial]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);
        const roles r                 = roles_of(net, order);
        const std::uint32_t dialing_a = net.a.e.attempt_count(r.dialed_on_a);
        const std::uint32_t dialing_b = net.b.e.attempt_count(r.dialed_on_b);

        CHECK(open_and_idle(net.a.e, r.loser_on_a));
        CHECK(open_and_idle(net.b.e, r.loser_on_b));
        net.publish_from(publisher_of(net, order));
        CHECK(subscriber_of(net, order).received.size() == 1);

        net.advance_by(k_past_window);
        CHECK(closed(net.a.e, r.loser_on_a));
        CHECK(closed(net.b.e, r.loser_on_b));
        CHECK(no_teardown_edge(net.a.edges.for_peer(r.loser_on_a)));
        CHECK(no_teardown_edge(net.b.edges.for_peer(r.loser_on_b)));
        CHECK(net.a.e.attempt_count(r.dialed_on_a) == dialing_a);
        CHECK(net.b.e.attempt_count(r.dialed_on_b) == dialing_b);
        CHECK(net.a.e.is_connected(r.kept_on_a));
        CHECK(net.b.e.is_connected(r.kept_on_b));
    }
}

TEST_CASE("crossed dial: no liveliness verdict or dead edge follows a resolution", "[integration][crossed_dial]")
{
    plexus::io::subscriber_qos qos{};
    qos.requested_lease_ns = static_cast<std::uint64_t>(std::chrono::nanoseconds(k_lease).count());
    for(const combine policy : k_policies)
        for(const crossing order : k_orders)
        {
            manual_clock::reset();
            liveliness_options live{};
            live.policy = policy;
            crossed_net net{live};
            net.cross(order, qos);
            const roles r            = roles_of(net, order);
            const std::size_t from_a = net.a.verdicts.events.size();
            const std::size_t from_b = net.b.verdicts.events.size();

            net.advance_by(k_past_miss);
            net.advance_by(k_past_window);
            CHECK(verdicts_for(net.a.verdicts, r.loser_on_a, from_a) == 0);
            CHECK(verdicts_for(net.b.verdicts, r.loser_on_b, from_b) == 0);
            CHECK(liveness_for(net.a, r.loser_on_a) == 0);
            CHECK(liveness_for(net.b, r.loser_on_b) == 0);
            CHECK(no_teardown_edge(net.a.edges.for_peer(r.loser_on_a)));
            CHECK(no_teardown_edge(net.b.edges.for_peer(r.loser_on_b)));
            CHECK(no_teardown_edge(net.a.edges.for_peer(r.kept_on_a)));
            CHECK(no_teardown_edge(net.b.edges.for_peer(r.kept_on_b)));
            CHECK(last_alive(net.a.verdicts, r.kept_on_a));
            CHECK(last_alive(net.b.verdicts, r.kept_on_b));
            net.publish_from(publisher_of(net, order));
            CHECK(subscriber_of(net, order).received.size() == 1);
        }
}

TEST_CASE("crossed dial: once its discovery awareness ages out, the discovered id of a peer kept through an accepted session reads lost while the "
          "survivor stays alive",
          "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net{opts(combine::any_signal_alive)};
    net.cross(crossing::incumbent_loses);
    REQUIRE(lost_for(net.b.verdicts, net.id_a) == 0);

    net.advance_by(std::chrono::nanoseconds(k_ttl) + 2 * k_step);
    CHECK(lost_for(net.b.verdicts, net.id_a) == 1);
    CHECK(lost_for(net.b.verdicts, inbound_slot(1)) == 0);
    CHECK(net.b.e.is_connected(inbound_slot(1)));
    CHECK(last_alive(net.b.verdicts, inbound_slot(1)));
    net.publish_from(net.a);
    CHECK(net.b.received.size() == 1);
}

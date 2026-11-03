#include "test_crossed_dial_common.h"

#include "plexus/io/endpoint_id.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace crossed_dial_fixture;
using plexus::wire::rpc_status;

namespace {

constexpr crossing k_orders[]   = {crossing::at_once, crossing::newcomer_loses, crossing::incumbent_loses};
constexpr auto k_past_ceiling   = k_window + std::chrono::milliseconds(2000) + k_step;
constexpr std::string_view k_rv = "crossed.both";

node_id dialed_on_a(const crossed_net &net, crossing order)
{
    return order == crossing::incumbent_loses ? endpoint_id(net.ep_b) : net.id_b;
}

node_id dialed_on_b(const crossed_net &net, crossing order)
{
    return order == crossing::newcomer_loses ? endpoint_id(net.ep_a) : net.id_a;
}

bool closed(engine &e, const node_id &slot)
{
    auto *s = e.session_for(slot);
    return s != nullptr && s->pending().closing();
}

void publish(crossed_net &net, node_side &side, const std::string &payload)
{
    side.e.publish(k_rv, std::as_bytes(std::span{payload}));
    net.drive();
}

std::int32_t answered_call(crossed_net &net, node_side &caller, const node_id &callee)
{
    std::int32_t answers = 0;
    caller.e.call(callee, k_rv, std::as_bytes(std::span{std::string_view{"ping"}}),
                  [&answers](rpc_status s, std::span<const std::byte>) { answers += s == rpc_status::success ? 1 : 0; });
    net.drive();
    return answers;
}

}

TEST_CASE("crossed dial: neither end redials after the pair resolves", "[integration][crossed_dial]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);
        const std::uint32_t dialing_a = net.a.e.attempt_count(dialed_on_a(net, order));
        const std::uint32_t dialing_b = net.b.e.attempt_count(dialed_on_b(net, order));

        net.advance_by(k_past_ceiling);
        CHECK(net.a.e.attempt_count(dialed_on_a(net, order)) == dialing_a);
        CHECK(net.b.e.attempt_count(dialed_on_b(net, order)) == dialing_b);
        CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
        CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
        CHECK(!net.a.e.has_session(inbound_slot(2)));
        CHECK(!net.b.e.has_session(inbound_slot(2)));
        CHECK(net.a.edges.for_peer(*kept_slot(net.a.e, net.id_b)).disconnected == 0);
        CHECK(net.b.edges.for_peer(*kept_slot(net.b.e, net.id_a)).disconnected == 0);
    }
}

TEST_CASE("crossed dial: a later reach for the peer does not dial again", "[integration][crossed_dial][!shouldfail]")
{
    manual_clock::reset();
    crossed_net net;
    net.cross(crossing::incumbent_loses);

    net.b.e.reach(net.id_a);
    net.drive();
    net.advance_by(k_past_ceiling);
    net.b.e.reach(net.id_a);
    net.drive();
    CHECK(!net.a.e.has_session(inbound_slot(2)));
    CHECK(resolution_lines(net.b.log).size() == 1);
    CHECK(net.b.e.registry().driver_for(net.id_a).redial_denied());
    CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
    CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
}

TEST_CASE("crossed dial: subscribe and call by discovered id reach the survivor in both directions", "[integration][crossed_dial][!shouldfail]")
{
    manual_clock::reset();
    crossed_net net;
    net.cross(crossing::incumbent_loses);
    net.a.e.procedures().serve(k_rv, [](std::span<const std::byte> param, auto &reply) { reply(rpc_status::success, param); });
    net.b.e.procedures().serve(k_rv, [](std::span<const std::byte> param, auto &reply) { reply(rpc_status::success, param); });

    net.a.e.subscribe(net.id_b, k_rv);
    net.b.e.subscribe(net.id_a, k_rv);
    net.drive();
    publish(net, net.a, "from a");
    publish(net, net.b, "from b");
    CHECK(net.a.received == std::vector<std::string>{"from b"});
    CHECK(net.b.received == std::vector<std::string>{"from a"});
    CHECK(answered_call(net, net.a, net.id_b) == 1);
    CHECK(answered_call(net, net.b, net.id_a) == 1);
}

TEST_CASE("crossed dial: a drop on a demoted loser closes it at once without a redial", "[integration][crossed_dial][!shouldfail]")
{
    manual_clock::reset();
    crossed_net net;
    net.cross(crossing::incumbent_loses);
    const std::uint32_t dialing_b = net.b.e.attempt_count(net.id_a);
    REQUIRE(!closed(net.b.e, net.id_a));

    net.a.e.session_for(inbound_slot(1))->tear_down();
    net.drive();
    CHECK(closed(net.b.e, net.id_a));
    CHECK(net.b.e.registry().driver_for(net.id_a).redial_denied());
    net.advance_by(k_past_ceiling);
    CHECK(net.b.e.attempt_count(net.id_a) == dialing_b);
    CHECK(net.b.edges.for_peer(net.id_a).disconnected == 0);
    CHECK(net.a.edges.for_peer(inbound_slot(1)).disconnected == 0);
    CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
    CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
}

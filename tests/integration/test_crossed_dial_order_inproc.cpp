#include "test_crossed_dial_common.h"

#include "plexus/io/endpoint_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using namespace crossed_dial_fixture;

namespace {

constexpr auto k_past_ceiling = k_window + std::chrono::milliseconds(2000) + k_step;

bool closed(engine &e, const node_id &slot)
{
    auto *s = e.session_for(slot);
    return s == nullptr || s->pending().closing();
}

void dial_incumbent_loses(crossed_net &net)
{
    net.b.e.note_peer(net.id_a, net.ep_a);
    net.b.e.subscribe(net.id_a, k_topic);
    net.advance_by(k_step);
    net.a.e.dial(net.ep_b);
}

struct step_marks
{
    std::optional<std::int32_t> completed;
    std::optional<std::int32_t> closed;
};

void step_and_mark(crossed_net &net, step_marks &marks, std::int32_t &n)
{
    while(net.ex.step())
    {
        ++n;
        if(!marks.completed && net.a.e.is_connected(endpoint_id(net.ep_b)))
            marks.completed = n;
        if(!marks.closed && closed(net.a.e, inbound_slot(1)))
            marks.closed = n;
    }
}

}

TEST_CASE("crossed dial: the remote's survivor completes before the loser's close reaches it", "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net;
    dial_incumbent_loses(net);
    const std::uint32_t dialing_a = net.a.e.attempt_count(endpoint_id(net.ep_b));
    const std::uint32_t dialing_b = net.b.e.attempt_count(net.id_a);

    step_marks marks;
    std::int32_t n = 0;
    step_and_mark(net, marks, n);
    REQUIRE(marks.completed.has_value());
    CHECK(!marks.closed.has_value());
    for(std::chrono::nanoseconds done{0}; done <= k_window; done += k_step)
    {
        manual_clock::advance(k_step);
        step_and_mark(net, marks, n);
    }
    REQUIRE(marks.closed.has_value());
    CHECK(*marks.completed < *marks.closed);
    CHECK(closed(net.b.e, net.id_a));
    CHECK(net.a.edges.for_peer(inbound_slot(1)).disconnected == 0);
    CHECK(net.a.edges.for_peer(endpoint_id(net.ep_b)).disconnected == 0);
    CHECK(net.b.edges.for_peer(net.id_a).disconnected == 0);
    CHECK(net.b.edges.for_peer(inbound_slot(1)).disconnected == 0);
    CHECK(net.a.e.attempt_count(endpoint_id(net.ep_b)) == dialing_a);
    CHECK(net.b.e.attempt_count(net.id_a) == dialing_b);
}

// The inproc bus is one FIFO for every channel, so a close sent after the survivor's response always arrives after
// it. The incumbent is torn down while the newcomer's request is still in flight instead, which leaves the remote in
// the same state: its accepted end complete and its dialed survivor not yet complete.
TEST_CASE("crossed dial: a loser end torn down early outside the protocol is an ordinary drop at the remote", "[integration][crossed_dial]")
{
    manual_clock::reset();
    crossed_net net;
    dial_incumbent_loses(net);
    const std::uint32_t dialing_a = net.a.e.attempt_count(endpoint_id(net.ep_b));
    const std::uint32_t dialing_b = net.b.e.attempt_count(net.id_a);
    while(!net.a.e.has_session(endpoint_id(net.ep_b)) && net.ex.step())
    {
    }
    REQUIRE(net.a.e.has_session(endpoint_id(net.ep_b)));
    REQUIRE(net.b.e.is_connected(net.id_a));

    net.b.e.session_for(net.id_a)->tear_down();
    net.drive();
    CHECK(net.a.edges.for_peer(inbound_slot(1)).disconnected == 1);
    CHECK(net.a.e.is_connected(endpoint_id(net.ep_b)));
    CHECK(complete_sessions_for(net.a.e, net.id_b) == 1);
    CHECK(complete_sessions_for(net.b.e, net.id_a) == 1);
    net.advance_by(k_past_ceiling);
    CHECK(net.a.e.attempt_count(endpoint_id(net.ep_b)) == dialing_a);
    CHECK(net.b.e.attempt_count(net.id_a) == dialing_b);
    CHECK(!net.a.e.has_session(inbound_slot(2)));
    CHECK(!net.b.e.has_session(inbound_slot(2)));
}

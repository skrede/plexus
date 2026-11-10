#include "test_crossed_dial_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

using namespace crossed_dial_fixture;
using plexus::io::peer_kind;

namespace {

constexpr auto k_past_window  = k_window + 2 * k_step;
constexpr auto k_deadline     = std::chrono::milliseconds(500);
constexpr crossing k_orders[] = {crossing::newcomer_loses, crossing::incumbent_loses};

std::size_t duplicate_lines(const recording_logger &log)
{
    std::size_t n = 0;
    for(const auto &line : log.lines)
        n += line.starts_with("plexus: duplicate session with ") ? 1 : 0;
    return n;
}

std::size_t missed_deadlines(const node_side &side, const node_id &id)
{
    std::size_t n = 0;
    for(const auto &ev : side.liveness)
        n += ev.endpoint == id && ev.kind == plexus::io::liveness_kind::missed_deadline ? 1 : 0;
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

std::optional<peer_kind> kept_kind_of(engine &e, const node_id &proven)
{
    std::optional<peer_kind> kind;
    std::size_t n = 0;
    e.registry().for_each_connected(
            [&](const node_id &, engine::session_type &s)
            {
                if(s.peer_identity() != proven)
                    return;
                ++n;
                kind = plexus::io::detail::session_kind(s);
            });
    return n == 1 ? kind : std::nullopt;
}

}

TEST_CASE("same-kind duplicate: a second accepted session for one identity replaces the first", "[integration][same_kind]")
{
    manual_clock::reset();
    crossed_net net;
    const node_id restarted   = make_id(k_restarted);
    const std::size_t slots_b = net.b.e.registry().slot_count();
    incarnation first{net};
    first.e.dial(net.ep_b);
    net.drive();
    REQUIRE(complete_sessions_for(net.b.e, restarted) == 1);

    incarnation second{net};
    second.e.dial(net.ep_b);
    net.drive();
    CHECK(complete_sessions_for(net.b.e, restarted) == 1);
    CHECK(duplicate_lines(net.b.log) == 1);
    net.advance_by(k_past_window);
    CHECK(second.e.is_connected(endpoint_id(net.ep_b)));
    CHECK(first.e.is_dead(endpoint_id(net.ep_b)));
    CHECK(!second.e.is_dead(endpoint_id(net.ep_b)));
    CHECK(net.b.e.is_connected(restarted));
    CHECK(net.b.edges.for_peer(restarted).disconnected == 0);
    CHECK(lost_for(net.b.verdicts, restarted) == 0);
    CHECK(last_alive(net.b.verdicts, restarted));
    CHECK(net.b.e.registry().slot_count() == slots_b + 1);
}

TEST_CASE("same-kind duplicate: a dial to a second endpoint of a host reached by card replaces the card's session", "[integration][same_kind]")
{
    manual_clock::reset();
    crossed_net net;
    const endpoint ep_b2{"inproc", "crossed-b-second"};
    net.b.e.listen(ep_b2);
    const std::size_t slots_a = net.a.e.registry().slot_count();
    const std::size_t slots_b = net.b.e.registry().slot_count();
    net.a.e.note_peer(net.id_b, net.ep_b);
    net.a.e.subscribe(net.id_b, k_topic);
    net.drive();
    REQUIRE(net.a.e.is_connected(net.id_b));

    net.a.e.dial(ep_b2);
    net.drive();
    CHECK(kept_slot(net.a.e, net.id_b) == std::optional<node_id>{endpoint_id(ep_b2)});
    CHECK(kept_kind_of(net.b.e, net.id_a) == peer_kind::accepted);
    CHECK(duplicate_lines(net.a.log) == 1);
    CHECK(duplicate_lines(net.b.log) == 1);
    net.advance_by(k_past_window);
    CHECK(net.a.e.registry().slot_count() == slots_a + 1);
    CHECK(net.b.e.registry().slot_count() == slots_b + 1);
    CHECK(net.a.e.is_connected(endpoint_id(ep_b2)));
    CHECK(net.a.edges.for_peer(endpoint_id(ep_b2)).disconnected == 0);
    CHECK(net.a.edges.for_peer(net.id_b).disconnected == 1);
    net.publish_from(net.b);
    CHECK(net.a.received.size() == 1);
}

TEST_CASE("same-kind duplicate: the loser's liveliness and monitor entries are retired before the survivor re-registers them", "[integration][same_kind]")
{
    manual_clock::reset();
    crossed_net net;
    const node_id restarted = make_id(k_restarted);
    plexus::io::subscriber_qos qos{};
    qos.requested_deadline_ns = static_cast<std::uint64_t>(std::chrono::nanoseconds(k_deadline).count());
    net.b.e.subscribe(restarted, k_topic, qos);
    incarnation first{net};
    first.e.dial(net.ep_b);
    net.drive();
    incarnation second{net};
    second.e.dial(net.ep_b);
    net.drive();
    REQUIRE(complete_sessions_for(net.b.e, restarted) == 1);

    net.advance_by(k_past_window);
    const std::size_t missed = missed_deadlines(net.b, restarted);
    CHECK(lost_for(net.b.verdicts, restarted) == 0);
    second.e.publish(k_topic, std::as_bytes(std::span{std::string_view{"one"}}));
    net.advance_by(k_deadline + 2 * k_step);
    CHECK(net.b.received.size() == 1);
    CHECK(missed_deadlines(net.b, restarted) == missed + 1);

    second.e.hang_up(net.ep_b);
    net.advance_by(k_deadline + 2 * k_step);
    CHECK(lost_for(net.b.verdicts, restarted) == 1);
    CHECK(missed_deadlines(net.b, restarted) == missed + 1);
}

TEST_CASE("same-kind duplicate: crossed pairs still keep the tie-break survivor in either completion order", "[integration][same_kind]")
{
    for(const crossing order : k_orders)
    {
        manual_clock::reset();
        crossed_net net;
        net.cross(order);
        CHECK(kept_kind_of(net.a.e, net.id_b) == peer_kind::dialed);
        CHECK(kept_kind_of(net.b.e, net.id_a) == peer_kind::accepted);
        CHECK(resolution_lines(net.a.log).size() == 1);
        CHECK(resolution_lines(net.b.log).size() == 1);
        CHECK(duplicate_lines(net.a.log) == 0);
        CHECK(duplicate_lines(net.b.log) == 0);
    }
}

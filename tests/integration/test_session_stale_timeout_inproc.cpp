#include "test_session_stale_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/disconnect_cause.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <vector>
#include <cstdint>
#include <string_view>

using namespace session_stale_fixture;
using plexus::io::combine;
using plexus::io::disconnect_cause;
using plexus::io::liveliness_verdict;

namespace {

constexpr std::chrono::nanoseconds k_default_teardown = std::chrono::seconds(10);
constexpr std::chrono::nanoseconds k_short_teardown   = std::chrono::seconds(2);
constexpr std::chrono::nanoseconds k_pause            = std::chrono::seconds(5);
constexpr std::chrono::nanoseconds k_late_step        = std::chrono::seconds(1);

struct peer_log final : plexus::io::observer
{
    explicit peer_log(const plexus::node_id &id)
            : lost(0)
            , alive(0)
            , watched(id)
            , causes()
    {
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        if(ev.id != watched)
            return;
        lost += ev.verdict == liveliness_verdict::lost ? 1 : 0;
        alive += ev.verdict == liveliness_verdict::alive ? 1 : 0;
    }

    void on_peer_disconnected_with_cause(const plexus::node_id &id, std::string_view, plexus::io::peer_kind, disconnect_cause cause) override
    {
        if(id == watched)
            causes.push_back(cause);
    }

    std::int32_t lost;
    std::int32_t alive;
    plexus::node_id watched;
    std::vector<disconnect_cause> causes;
};

bool connected_throughout(stale_pair &net, std::chrono::nanoseconds from, std::chrono::nanoseconds until)
{
    bool kept = true;
    for(std::chrono::nanoseconds t = from; t < until; t += k_tick_granularity)
    {
        net.advance(k_tick_granularity);
        kept = kept && net.a.is_connected(net.id_b);
    }
    return kept;
}

void expect_alive_again_on(stale_pair &net, const peer_log &log, const stale_engine::session_type *session)
{
    const std::int32_t alive = log.alive;
    net.transport_b.silence(mute::none);
    net.tick_until([&] { return log.alive > alive; }, miss_window());
    CHECK(log.alive == alive + 1);
    CHECK(net.a.session_for(net.id_b) == session);
}

void expect_torn_down_at(stale_pair &net, const peer_log &log, std::chrono::nanoseconds timeout)
{
    net.transport_b.silence(mute::all);
    net.tick_until([] { return false; }, timeout - k_tick_granularity);
    CHECK(net.a.is_connected(net.id_b));
    CHECK(log.causes.empty());

    net.tick_until([&] { return !log.causes.empty(); }, 2 * k_tick_granularity);
    REQUIRE(log.causes.size() == 1);
    CHECK(log.causes.front() == disconnect_cause::heartbeat_timeout);
    CHECK_FALSE(net.a.is_connected(net.id_b));
}

}

TEST_CASE("session stale: with the default options and the all_required policy a peer silent for a five-second pause reads lost but keeps its session", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(with_policy(combine::all_required));
    peer_log log(net.id_b);
    net.a.add_observer(log);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto *session = net.a.session_for(net.id_b);
    const auto attempts = net.a.attempt_count(net.id_b);
    net.tick_until([] { return false; }, 2 * k_tick_granularity);

    net.transport_b.silence(mute::all);
    const auto to_lost = net.tick_until([&] { return log.lost > 0; }, k_pause);
    CHECK(log.lost == 1);
    CHECK(to_lost <= miss_window() + k_tick_granularity);
    CHECK(connected_throughout(net, to_lost, k_pause));
    CHECK(log.causes.empty());
    CHECK(net.a.attempt_count(net.id_b) == attempts);
    CHECK(net.a.session_for(net.id_b) == session);
    expect_alive_again_on(net, log, session);
}

TEST_CASE("session stale: with the default options a silent peer is torn down once the ten-second teardown timeout passes", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net;
    peer_log log(net.id_b);
    net.a.add_observer(log);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto attempts = net.a.attempt_count(net.id_b);

    expect_torn_down_at(net, log, k_default_teardown);
    net.tick_until([&] { return net.a.attempt_count(net.id_b) > attempts; }, miss_window());
    CHECK(net.a.attempt_count(net.id_b) > attempts);
    CHECK(log.causes.size() == 1);
}

TEST_CASE("session stale: a configured teardown timeout tears a silent peer down at that timeout", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(with_teardown(k_short_teardown));
    peer_log log(net.id_b);
    net.a.add_observer(log);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));

    expect_torn_down_at(net, log, k_short_teardown);
}

TEST_CASE("session stale: a clock jump longer than the teardown timeout tears no live peer down", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(with_teardown(k_short_teardown));
    peer_log log_a(net.id_b);
    peer_log log_b(net.id_a);
    net.a.add_observer(log_a);
    net.b.add_observer(log_b);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto *session = net.a.session_for(net.id_b);

    net.advance(k_short_teardown + k_tick_granularity);
    net.tick_until([] { return false; }, 2 * miss_window());
    CHECK(log_a.causes.empty());
    CHECK(log_b.causes.empty());
    CHECK(net.a.is_connected(net.id_b));
    CHECK(net.a.session_for(net.id_b) == session);
}

TEST_CASE("session stale: a tick late by less than the teardown timeout still sweeps a silent peer", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(with_teardown(k_short_teardown));
    peer_log log(net.id_b);
    net.a.add_observer(log);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));

    net.transport_b.silence(mute::all);
    net.tick_until([] { return false; }, k_short_teardown - std::chrono::milliseconds(500));
    REQUIRE(log.causes.empty());
    net.advance(k_late_step);
    REQUIRE(log.causes.size() == 1);
    CHECK(log.causes.front() == disconnect_cause::heartbeat_timeout);
}

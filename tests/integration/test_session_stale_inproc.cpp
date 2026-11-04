#include "recording_observer.h"
#include "test_session_stale_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

using namespace session_stale_fixture;
using plexus::io::combine;

namespace {

constexpr std::string_view k_topic = "/stale/topic";

struct lost_count final : plexus::io::observer
{
    explicit lost_count(const plexus::node_id &id)
            : lost(0)
            , watched(id)
    {
    }

    bool observes_liveliness() const override
    {
        return true;
    }

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        if(ev.id == watched && ev.verdict == plexus::io::liveliness_verdict::lost)
            ++lost;
    }

    std::int32_t lost;
    plexus::node_id watched;
};

plexus::io::liveliness_options with_policy(combine policy)
{
    plexus::io::liveliness_options live{};
    live.policy = policy;
    return live;
}

}

TEST_CASE("session stale: a dialed peer that falls silent is torn down within the miss window and redialed", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net;
    recording_observer rec;
    net.a.add_observer(rec);
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto attempts = net.a.attempt_count(net.id_b);
    net.transport_b.silence(mute::all);
    const auto elapsed = net.tick_until([&] { return rec.for_peer(net.id_b).disconnected > 0; }, 4 * miss_window());
    CHECK(rec.for_peer(net.id_b).disconnected == 1);
    CHECK(elapsed <= miss_window() + k_tick_granularity);
    CHECK_FALSE(net.a.is_connected(net.id_b));
    net.tick_until([&] { return net.a.attempt_count(net.id_b) > attempts; }, miss_window());
    CHECK(net.a.attempt_count(net.id_b) > attempts);
    CHECK(rec.for_peer(net.id_b).disconnected == 1);
}

TEST_CASE("session stale: an accepted peer that falls silent is torn down and its slot reclaimed", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net;
    recording_observer rec;
    net.b.add_observer(rec);
    const auto baseline = net.b.registry().slot_count();
    net.connect();
    REQUIRE(net.b.is_connected(net.id_a));
    net.a.forget(net.id_b);
    net.transport_a.silence(mute::all);
    const auto elapsed = net.tick_until([&] { return rec.for_peer(net.id_a).disconnected > 0; }, 4 * miss_window());
    CHECK(rec.for_peer(net.id_a).disconnected == 1);
    CHECK(elapsed <= miss_window() + k_tick_granularity);
    CHECK_FALSE(net.b.is_connected(net.id_a));
    net.advance(k_tick_granularity);
    CHECK(net.b.registry().slot_count() == baseline);
}

TEST_CASE("session stale: every fusion policy reads the silent peer lost once it is torn down", "[integration][session_stale]")
{
    for(const combine policy : {combine::any_signal_alive, combine::session_authoritative, combine::all_required})
    {
        manual_clock::reset();
        stale_pair net(with_policy(policy));
        const plexus::node_id key = plexus::io::endpoint_id(net.ep_b);
        lost_count probe(key);
        recording_observer rec;
        net.a.add_observer(probe);
        net.a.add_observer(rec);
        net.a.dial(net.ep_b);
        net.ex.drain();
        REQUIRE(net.a.is_connected(key));
        net.transport_b.silence(mute::all);
        net.tick_until([&] { return probe.lost > 0 && rec.for_peer(key).disconnected > 0; }, 4 * miss_window());
        CHECK(rec.for_peer(key).disconnected == 1);
        CHECK(probe.lost == 1);
        net.a.remove_observer(rec);
        net.a.remove_observer(probe);
    }
}

TEST_CASE("session stale: a peer that keeps sending data but whose heartbeats are lost is not torn down", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net;
    recording_observer rec;
    std::int32_t received = 0;
    net.a.add_observer(rec);
    net.a.on_message_route([&received](std::string_view, std::span<const std::byte>, const plexus::io::message_info &) { ++received; });
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.subscribe(net.id_b, k_topic);
    net.ex.drain();
    REQUIRE(net.a.is_connected(net.id_b));
    net.transport_b.silence(mute::heartbeats);
    const std::string payload = "tick";
    for(std::int32_t tick = 0; tick < 8 * miss_window() / k_tick_granularity; ++tick)
    {
        net.b.publish(k_topic, as_bytes(payload));
        net.advance(k_tick_granularity);
    }
    CHECK(net.a.is_connected(net.id_b));
    CHECK(rec.for_peer(net.id_b).disconnected == 0);
    CHECK(received >= 8 * miss_window() / k_tick_granularity);
}

#include "recording_observer.h"
#include "test_session_stale_tap.h"
#include "test_session_stale_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/disconnect_cause.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/rpc_status.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

using namespace session_stale_fixture;
using plexus::io::disconnect_cause;
using plexus::wire::msg_type;

namespace {

constexpr std::string_view k_procedure = "/stale/procedure";

struct seen_cause
{
    plexus::node_id id;
    disconnect_cause cause;
};

struct cause_log final : plexus::io::observer
{
    void on_peer_disconnected_with_cause(const plexus::node_id &id, std::string_view, plexus::io::peer_kind, disconnect_cause cause) override
    {
        seen.push_back(seen_cause{id, cause});
    }

    std::vector<seen_cause> seen;
};

std::int32_t frames_of(const relay &r, msg_type type)
{
    std::int32_t n = 0;
    for(const sent_frame &f : r.sent())
        n += f.type == type ? 1 : 0;
    return n;
}

void tear_down_b(stale_pair &net, const recording_observer &rec)
{
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    net.transport_b.silence(mute::all);
    net.tick_until([&] { return rec.for_peer(net.id_b).disconnected > 0; }, 4 * miss_window());
    REQUIRE(rec.for_peer(net.id_b).disconnected == 1);
}

}

TEST_CASE("session stale: the disconnect names the heartbeat timeout and is reported once", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(teardown_at_miss_window());
    recording_observer rec;
    cause_log causes_a;
    cause_log causes_b;
    net.a.add_observer(rec);
    net.a.add_observer(causes_a);
    net.b.add_observer(causes_b);
    tear_down_b(net, rec);
    net.tick_until([] { return false; }, 2 * miss_window());
    REQUIRE(causes_a.seen.size() == 1);
    CHECK(causes_a.seen.front().id == net.id_b);
    CHECK(causes_a.seen.front().cause == disconnect_cause::heartbeat_timeout);
    CHECK(rec.for_peer(net.id_b).disconnected == 1);
    REQUIRE_FALSE(causes_b.seen.empty());
    CHECK(causes_b.seen.front().id == net.id_a);
    CHECK(causes_b.seen.front().cause == disconnect_cause::unspecified);
}

TEST_CASE("session stale: nothing is sent to the silent peer at the teardown", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(teardown_at_miss_window());
    recording_observer rec;
    net.a.add_observer(rec);
    tear_down_b(net, rec);
    const auto torn_at = manual_clock::now();
    net.tick_until([] { return false; }, 2 * miss_window());
    std::int32_t in_teardown_tick = 0;
    std::int32_t heartbeats       = 0;
    std::int32_t later            = 0;
    for(const sent_frame &f : net.transport_a.relay_at(0).sent())
    {
        in_teardown_tick += f.at == torn_at ? 1 : 0;
        heartbeats += f.at == torn_at && f.type == msg_type::heartbeat ? 1 : 0;
        later += f.at > torn_at ? 1 : 0;
    }
    CHECK(in_teardown_tick == 1);
    CHECK(heartbeats == 1);
    CHECK(later == 0);
}

TEST_CASE("session stale: a request issued after the teardown is answered without reaching the torn session", "[integration][session_stale]")
{
    manual_clock::reset();
    stale_pair net(teardown_at_miss_window());
    recording_observer rec;
    net.a.add_observer(rec);
    tear_down_b(net, rec);
    const std::size_t dropped = net.a.forward_rpc_dropped_count();
    std::optional<plexus::wire::rpc_status> status;
    net.a.call(net.id_b, k_procedure, {}, [&status](plexus::wire::rpc_status st, std::span<const std::byte>) { status = st; }, std::chrono::milliseconds(200));
    net.ex.drain();
    CHECK(net.a.forward_rpc_dropped_count() == dropped + 1);
    net.tick_until([&] { return status.has_value(); }, miss_window());
    REQUIRE(status.has_value());
    CHECK(*status != plexus::wire::rpc_status::success);
    std::int32_t requests = 0;
    for(std::size_t n = 0; n < net.transport_a.relay_count(); ++n)
        requests += frames_of(net.transport_a.relay_at(n), msg_type::rpc_request) + frames_of(net.transport_a.relay_at(n), msg_type::forwarded);
    CHECK(requests == 0);
}

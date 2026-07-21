// The phase acceptance: a relay owning TWO live transport sessions carries BOTH control and data across
// the second live session end-to-end. A serial-attached origin, a relay, and a TCP-attached consumer
// stand up over a real openpty serial link (session A) and a real TCP loopback (session B) with NO
// direct origin<->consumer path. The consumer enumerates the origin from a peer_report that crossed
// session B's wire (no hand-delivery), receives the origin's publish re-originated by the relay with the
// origin as the delivered source, and — the request/response leg — calls the origin's procedure through
// the relay. Killing the origin withdraws the relayed peer. Reproduced over independent cold runs, each
// re-establishing BOTH sessions from scratch.

#include "test_forward_serial_e2e_common.h"

#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/endpoint_seam.h"
#include "plexus/io/message_info.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <string_view>

using namespace forward_serial_e2e_fixture;

namespace {

constexpr std::string_view k_topic     = "sensor/telemetry";
constexpr std::string_view k_procedure = "sensor/echo";
constexpr int k_runs                   = 3;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// The proven session's epoch id (0 if none) — distinct, nonzero per cold establishment.
std::uint64_t session_epoch(relay_node &n, const node_id &proven)
{
    std::uint64_t epoch = 0;
    n.router().registry().for_each_connected([&](const node_id &, auto &s) { if(s.peer_identity() == proven) epoch = s.session_id(); });
    return epoch;
}

struct delivery
{
    std::string body;
    std::optional<node_id> source;
};

void run_pubsub_once(std::vector<std::uint64_t> &epochs)
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    std::optional<plexus::publisher<>> pub;
    pub.emplace(*cluster.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);
    cluster.pump([&] { return cluster.relay->count_publishers(std::string{k_topic}) >= 1; });
    REQUIRE(cluster.relay->count_publishers(std::string{k_topic}) >= 1);

    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));

    const std::uint64_t epoch = session_epoch(*cluster.relay, cluster.consumer_id);
    REQUIRE(epoch != 0);
    epochs.push_back(epoch);

    // Control plane: the origin is enumerated from a report that crossed session B (never hand-delivered).
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });
    const auto *reported = find_participant(cluster.consumer, cluster.origin_id);
    REQUIRE(reported != nullptr);
    REQUIRE(reported->origin.how == graph::observation::reported);
    REQUIRE(reported->reach.via == cluster.relay_id);

    delivery got;
    plexus::subscriber<> sub{cluster.consumer, k_topic,
                             [&](std::span<const std::byte> b, const plexus::io::message_info &info)
                             {
                                 got.body = to_string(b);
                                 if(info.source_identity)
                                     got.source = info.source_identity->node_id();
                             }};
    // The consumer's demand must propagate up through the relay onto the origin's session before the
    // origin publishes — otherwise the publish predates the subscriber and is dropped at the source.
    cluster.pump([&] { return cluster.origin->count_subscribers(std::string{k_topic}) >= 1; });
    REQUIRE(cluster.origin->count_subscribers(std::string{k_topic}) >= 1);

    // Data plane: the origin's plain publish is re-originated by the relay and delivered with the origin
    // (not the relay) as the source.
    pub->publish(as_bytes(std::string{"24.5C"}));
    cluster.pump([&] { return !got.body.empty(); });
    REQUIRE(got.body == "24.5C");
    REQUIRE(got.source.has_value());
    REQUIRE(*got.source == cluster.origin_id);

    // No direct leaf-to-leaf path: the whole exchange rode the relay.
    REQUIRE_FALSE(connected(cluster.consumer, cluster.origin_id));
    REQUIRE_FALSE(connected(*cluster.origin, cluster.consumer_id));

    // Kill-origin withdraws the relayed peer over session B. The origin-bound publisher is retired
    // first so its handle never outlives the node it targets.
    pub.reset();
    cluster.kill_origin();
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) == nullptr; });
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) == nullptr);
}

void run_reqres_once()
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    std::string served;
    plexus::procedure<> echo{*cluster.origin, k_procedure,
                             [&](std::span<const std::byte> param, plexus::io::reply_fn &reply)
                             {
                                 served = to_string(param);
                                 reply(plexus::wire::rpc_status::success, param);
                             }};
    cluster.pump([&] { return cluster.relay->count_publishers(std::string{k_procedure}) >= 0; });

    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    std::optional<bool> ok;
    std::string response;
    plexus::caller<> call{cluster.consumer, k_procedure};
    call.call(as_bytes(std::string{"ping"}),
              [&](plexus::expected<plexus::reply, std::error_code> result)
              {
                  ok = result.has_value();
                  if(result)
                      response = to_string(result->bytes);
              });
    cluster.pump([&] { return ok.has_value(); });

    // Request/response transits the origin through the relay with a success reply carrying the bytes.
    REQUIRE(ok.has_value());
    REQUIRE(*ok);
    REQUIRE(response == "ping");
    REQUIRE(served == "ping");
    REQUIRE_FALSE(connected(cluster.consumer, cluster.origin_id));
}

}

TEST_CASE("pub/sub and control transit a serial+TCP relay end-to-end with the origin as source, over cold runs",
          "[integration][serial][relay][e2e]")
{
    std::vector<std::uint64_t> epochs;
    for(int run = 0; run < k_runs; ++run)
        run_pubsub_once(epochs);
    REQUIRE(epochs.size() == static_cast<std::size_t>(k_runs));
}

TEST_CASE("request/response transits a serial+TCP relay end-to-end returning success, over cold runs",
          "[integration][serial][relay][e2e][reqres]")
{
    for(int run = 0; run < k_runs; ++run)
        run_reqres_once();
}

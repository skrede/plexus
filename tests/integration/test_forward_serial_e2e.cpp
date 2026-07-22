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
#include "plexus/io/observer.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <algorithm>
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

// Records the consumer's graph deltas and the direct-peer liveliness verdicts so the kill-relay probe
// can read, at the public observer surface, WHICH change_kind crosses for an identity and whether the
// arbiter ever renders a verdict for a given peer — never by hand-poking a table or a verdict.
struct probe_observer final : plexus::io::observer
{
    std::vector<graph::graph_change> deltas;
    std::vector<plexus::io::peer_liveliness_event> verdicts;

    void on_graph_delta(const graph::graph_change &c) override { deltas.push_back(c); }
    void on_peer_liveliness(const plexus::io::peer_liveliness_event &e) override { verdicts.push_back(e); }
    bool observes_graph() const override { return true; }
    bool observes_liveliness() const override { return true; }
};

int kind_count(const probe_observer &o, const node_id &who, graph::change_kind kind)
{
    return static_cast<int>(std::count_if(o.deltas.begin(), o.deltas.end(),
                                          [&](const graph::graph_change &c) { return c.who == who && c.kind == kind; }));
}

bool arbiter_saw(const probe_observer &o, const node_id &who)
{
    return std::any_of(o.verdicts.begin(), o.verdicts.end(),
                       [&](const plexus::io::peer_liveliness_event &e) { return e.id == who; });
}

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

// A never consumer admits the relayed origin (it is enumerable) but never consumes its relayed publish:
// the demand still propagates up through the relay (so the origin publishes and the relay forwards the
// frame), yet the consumer drops it at the forwarded-receive rather than delivering it to a subscriber.
void run_never_denies_relayed_pubsub()
{
    cold_cluster cluster{plexus::io::route_usage::never};
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    std::optional<plexus::publisher<>> pub;
    pub.emplace(*cluster.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);
    cluster.pump([&] { return cluster.relay->count_publishers(std::string{k_topic}) >= 1; });

    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));

    // Admit-but-never-select: the relayed origin is enumerable at the consumer even under never.
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
    // The relay does receive the demand and forward the publish (the origin gains a subscriber), so the
    // frame reaches the consumer's forwarded-receive — where the never gate drops it before delivery.
    cluster.pump([&] { return cluster.origin->count_subscribers(std::string{k_topic}) >= 1; });
    pub->publish(as_bytes(std::string{"24.5C"}));
    serial_fixture::settle(cluster.io, std::chrono::milliseconds(150));

    REQUIRE(got.body.empty());
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) != nullptr);

    pub.reset();
    cluster.kill_origin();
}

// A never consumer's via-relay call fallback never arms, so a procedure reachable only through the relay
// finds no provider: the origin's handler never runs and the caller fails.
void run_never_call_finds_no_relayed_provider()
{
    cold_cluster cluster{plexus::io::route_usage::never};
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
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    std::optional<bool> ok;
    plexus::call_options opts;
    opts.deadline = std::chrono::milliseconds(500);
    plexus::caller<> call{cluster.consumer, k_procedure};
    call.call(as_bytes(std::string{"ping"}), opts, [&](plexus::expected<plexus::reply, std::error_code> result) { ok = result.has_value(); });
    cluster.pump([&] { return ok.has_value(); });

    REQUIRE(ok.has_value());
    REQUIRE_FALSE(*ok);
    REQUIRE(served.empty());
    REQUIRE_FALSE(connected(cluster.consumer, cluster.origin_id));
}

// An allow_relayed consumer reaches the same relayed origin the never consumer refuses: the publish is
// delivered with the origin as the source, exactly as the default prefer_direct consumer sees it.
void run_allow_relayed_reaches_relayed_origin()
{
    cold_cluster cluster{plexus::io::route_usage::allow_relayed};
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    std::optional<plexus::publisher<>> pub;
    pub.emplace(*cluster.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);
    cluster.pump([&] { return cluster.relay->count_publishers(std::string{k_topic}) >= 1; });

    cluster.bring_up_tcp();
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    delivery got;
    plexus::subscriber<> sub{cluster.consumer, k_topic,
                             [&](std::span<const std::byte> b, const plexus::io::message_info &info)
                             {
                                 got.body = to_string(b);
                                 if(info.source_identity)
                                     got.source = info.source_identity->node_id();
                             }};
    cluster.pump([&] { return cluster.origin->count_subscribers(std::string{k_topic}) >= 1; });
    pub->publish(as_bytes(std::string{"24.5C"}));
    cluster.pump([&] { return !got.body.empty(); });

    REQUIRE(got.body == "24.5C");
    REQUIRE(got.source.has_value());
    REQUIRE(*got.source == cluster.origin_id);

    pub.reset();
    cluster.kill_origin();
}

// A declining origin is never announced downstream. Its decline rides its heartbeats (level, not the
// handshake), so the relay lifts it on serial-ready and retracts it on the first decline heartbeat; a
// non-declining second origin still appears at the consumer — the positive control that the absence is
// the decline, not a broken fixture.
void run_origin_decline_suppresses_announcement()
{
    cold_cluster cluster{plexus::io::route_usage::prefer_direct, plexus::io::route_usage::never};
    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));

    cluster.pump([&] { return cluster.relay->router().peer_declines(cluster.origin_id); });
    REQUIRE_FALSE(cluster.relay->router().reports_origin(cluster.origin_id)); // withdrawn as an origin

    cluster.bring_up_tcp();
    cluster.bring_up_origin2();

    // Positive control: the non-declining second origin is announced to the consumer over the relay.
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin2_id) != nullptr; });
    const auto *control = find_participant(cluster.consumer, cluster.origin2_id);
    REQUIRE(control != nullptr);
    REQUIRE(control->origin.how == graph::observation::reported);
    REQUIRE(control->reach.via == cluster.relay_id);

    // The declining origin is never offered to the consumer.
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) == nullptr);

    cluster.kill_origin();
}

// Flipping an origin's decline mid-session withdraws it downstream: the consumer sees it reported over
// the relay, the origin flips its route-usage to never, and the relay retracts it on the next heartbeat.
void run_origin_decline_flip_withdraws_downstream()
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    cluster.bring_up_tcp();

    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) != nullptr);

    cluster.origin->router().set_route_usage(plexus::io::route_usage::never);

    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) == nullptr; });
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) == nullptr);

    cluster.kill_origin();
}

// A declining consumer is offered no relayed path: the relay lifts the origin (the positive control that
// it HAS the origin reported) but skips the declining consumer's session, so the origin never enters the
// decliner's participants(). The consumer declines before the origin is reported, so no session-ready
// replay races ahead of the decline.
void run_consumer_decline_receives_no_offer()
{
    cold_cluster cluster{plexus::io::route_usage::never};
    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));

    cluster.pump([&] { return cluster.relay->router().peer_declines(cluster.consumer_id); });

    cluster.bring_up_serial();
    cluster.pump([&] { return cluster.relay->router().reports_origin(cluster.origin_id); });
    REQUIRE(cluster.relay->router().reports_origin(cluster.origin_id)); // positive control: the relay HAS it

    serial_fixture::settle(cluster.io, std::chrono::milliseconds(150)); // let any (skipped) broadcast land
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) == nullptr);

    cluster.kill_origin();
}

// A per-sequence delivery ledger read at the consumer facade: every delivered publication_sequence is
// recorded so exactly-once (no duplicate seq) and contiguity (no gap) are assertions over the ledger.
// A duplicate delivery of a superseded relayed frame shows up as a repeated seq; a lost frame as a gap.
struct seq_ledger
{
    std::vector<std::uint64_t> seqs;
    std::optional<node_id> last_source;
};

bool exactly_once(const seq_ledger &l)
{
    std::vector<std::uint64_t> s = l.seqs;
    std::sort(s.begin(), s.end());
    return std::adjacent_find(s.begin(), s.end()) == s.end();
}

bool contiguous(const seq_ledger &l)
{
    if(l.seqs.empty())
        return true;
    std::vector<std::uint64_t> s = l.seqs;
    std::sort(s.begin(), s.end());
    return s.back() - s.front() + 1 == s.size();
}

void publish_and_drain(dualhome_cluster &c, plexus::publisher<> &pub, seq_ledger &log, const std::string &body)
{
    const std::size_t before = log.seqs.size();
    pub.publish(as_bytes(body));
    c.pump([&] { return log.seqs.size() > before; });
    serial_fixture::settle(c.io, std::chrono::milliseconds(40)); // let any duplicate (bug) or forwarded twin land
}

// The criterion-2 discharge: a live publish stream survives a relayed<->direct switchover to the same
// origin identity with exactly-once delivery. Relay-first, then the direct session supersedes the relayed
// delivery (no duplicate), then the direct drop re-arms the relayed path (no gap) — proving the relay was
// forwarding-but-suppressed all along, since resume delivers with nothing re-anchored.
void run_switchover_relay_first()
{
    dualhome_cluster c;
    std::optional<plexus::publisher<>> pub;
    pub.emplace(*c.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);

    c.bring_up_relay_to_origin();
    c.bring_up_consumer_to_relay();
    c.pump([&] { return find_participant(c.consumer, c.origin_id) != nullptr; });

    seq_ledger log;
    plexus::subscriber<> sub{c.consumer, k_topic,
                             [&](std::span<const std::byte> b, const plexus::io::message_info &info)
                             {
                                 (void)b;
                                 log.seqs.push_back(info.publication_sequence);
                                 if(info.source_identity)
                                     log.last_source = info.source_identity->node_id();
                             }};
    c.pump([&] { return c.origin->count_subscribers(std::string{k_topic}) >= 1; });

    publish_and_drain(c, *pub, log, "r0"); // relay-only delivery
    publish_and_drain(c, *pub, log, "r1");
    REQUIRE(log.seqs.size() == 2);

    c.bring_up_consumer_to_origin_direct();
    // The direct path must be WORKING (its subscription reached the origin, so both paths fan) before the
    // stream is driven: criterion 2 is that a relayed route never displaces a working direct path.
    c.pump([&] { return c.origin->count_subscribers(std::string{k_topic}) >= 2; });
    publish_and_drain(c, *pub, log, "d0"); // direct delivers, the forwarded twin is suppressed
    publish_and_drain(c, *pub, log, "d1");
    REQUIRE(log.seqs.size() == 4); // no duplicate from the still-forwarding relay

    c.drop_direct(); // the direct path is lost; the relayed path re-arms
    publish_and_drain(c, *pub, log, "x0");
    publish_and_drain(c, *pub, log, "x1");
    REQUIRE(log.seqs.size() == 6); // resume delivered, nothing lost, nothing doubled

    REQUIRE(exactly_once(log));
    REQUIRE(contiguous(log));
    REQUIRE(log.last_source.has_value());
    REQUIRE(*log.last_source == c.origin_id);

    pub.reset();
}

// The direct-first ordering arm: the direct session is live BEFORE the relay reports the origin, so the
// session-ready arm cannot fire on it (the origin was not yet reported). Suppression must instead arm at
// report ingest when a live direct session already exists — otherwise the relayed publishes double-deliver.
void run_switchover_direct_first()
{
    dualhome_cluster c;
    std::optional<plexus::publisher<>> pub;
    pub.emplace(*c.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);

    c.bring_up_consumer_to_origin_direct(); // direct live FIRST, origin not yet reported

    seq_ledger log;
    plexus::subscriber<> sub{c.consumer, k_topic,
                             [&](std::span<const std::byte> b, const plexus::io::message_info &info)
                             {
                                 (void)b;
                                 log.seqs.push_back(info.publication_sequence);
                                 if(info.source_identity)
                                     log.last_source = info.source_identity->node_id();
                             }};
    c.pump([&] { return c.origin->count_subscribers(std::string{k_topic}) >= 1; });

    publish_and_drain(c, *pub, log, "d0"); // direct-only delivery
    REQUIRE(log.seqs.size() == 1);

    c.bring_up_relay_to_origin();  // the relay now reports the origin AND forwards its publishes
    c.bring_up_consumer_to_relay();
    c.pump([&] { return find_participant(c.consumer, c.origin_id) != nullptr; });
    c.pump([&] { return c.origin->count_subscribers(std::string{k_topic}) >= 2; });

    publish_and_drain(c, *pub, log, "d1"); // ingest arm must suppress the forwarded twin
    publish_and_drain(c, *pub, log, "d2");
    REQUIRE(log.seqs.size() == 3);

    REQUIRE(exactly_once(log));
    REQUIRE(contiguous(log));
    // Source provenance by id is pinned on the relayed path in the switchover test; the last delivery here
    // rides the direct (endpoint-dialed) session, which labels the source with its provisional endpoint id,
    // so only source-identity presence is asserted.
    REQUIRE(log.last_source.has_value());

    pub.reset();
}

// A composed relay<> node discloses it OFFERS a relayed path, and its reported-origin count moves with
// a real serial lift: 0 before the origin is attached, >=1 once the relay has lifted it. The disclosure
// reaches the logger at least once (never silent), and offering is a compile-time property of the
// profile — a relay<> node offers regardless of whether any origin is yet reported.
void run_relay_self_check_discloses_offering()
{
    cold_cluster cluster;

    const plexus::relay_posture before = cluster.relay->self_check();
    REQUIRE(before.offering_relay);
    REQUIRE(before.reported_origins == 0);

    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));
    cluster.pump([&] { return cluster.relay->router().reports_origin(cluster.origin_id); });

    const plexus::relay_posture after = cluster.relay->self_check();
    REQUIRE(after.offering_relay);
    REQUIRE(after.reported_origins >= 1);
    REQUIRE(after.reported_origins > before.reported_origins);
    REQUIRE(cluster.relay_log.self_checks >= 1);

    cluster.kill_origin();
}

// A non-relay leaf discloses N>0 routes transiting a relay only AFTER a real peer_report crosses the
// wire — the origin enters its table reachable via the relay. It discloses offering_relay == false (the
// null emitter twin, no offering code instantiated) and its disclosure reaches the logger at least once.
void run_leaf_self_check_discloses_routes_via_relay()
{
    cold_cluster cluster;
    cluster.bring_up_serial();
    cluster.bring_up_tcp();

    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    const plexus::relay_posture posture = cluster.consumer.self_check();
    REQUIRE_FALSE(posture.offering_relay);
    REQUIRE(posture.reported_origins == 0);
    REQUIRE(posture.routes_via_relays >= 1);
    REQUIRE(cluster.consumer_log.self_checks >= 1);

    cluster.kill_origin();
}

// The wave-0 probe. It PINS today's consumer-side behavior when the whole relay node dies over the
// real relayed path (the derivation-design input, open question Q2): with the origin reachable ONLY
// via the relay, kill the relay and, within a bounded pump, record at the consumer's public surface
// (a) whether the origin's via-relay row is retired or left stale, (b) which change_kind — if any —
// crosses for the origin, (c) whether the direct-peer liveliness arbiter renders any verdict for the
// via-only origin (it must not — a reported identity never feeds the arbiter), and (d) that a
// via-relay call through the dead relay fails cleanly, bounded, with the forward-rpc drop counted.
// The recorded answer — not a desired future behavior — decides where the liveliness derivation hooks
// and how unreachable state is stored. It documents which of prompt-retire-to-disappeared vs
// stale-window is TRUE today.
void run_kill_relay_probe()
{
    probe_observer obs; // declared before the cluster so it outlives every posted fan-out into it
    cold_cluster cluster;
    cluster.consumer.router().add_observer(obs);

    std::string served;
    plexus::procedure<> echo{*cluster.origin, k_procedure,
                             [&](std::span<const std::byte> param, plexus::io::reply_fn &reply)
                             {
                                 served = to_string(param);
                                 reply(plexus::wire::rpc_status::success, param);
                             }};

    cluster.bring_up_serial();
    REQUIRE(connected(*cluster.relay, cluster.origin_id));
    cluster.bring_up_tcp();
    REQUIRE(connected(cluster.consumer, cluster.relay_id));

    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });
    const auto *before = find_participant(cluster.consumer, cluster.origin_id);
    REQUIRE(before != nullptr);
    REQUIRE(before->origin.how == graph::observation::reported);
    REQUIRE(before->reach.via == cluster.relay_id);
    REQUIRE(kind_count(obs, cluster.origin_id, graph::change_kind::appeared) >= 1);
    REQUIRE_FALSE(arbiter_saw(obs, cluster.origin_id)); // a via-only origin never feeds the arbiter

    const std::size_t drops_before = cluster.consumer.router().forward_rpc_dropped_count();

    cluster.kill_relay();
    cluster.pump([&] { return !connected(cluster.consumer, cluster.relay_id); });
    serial_fixture::settle(cluster.io, std::chrono::milliseconds(200));

    const bool relay_session_gone   = !connected(cluster.consumer, cluster.relay_id);
    const auto *after               = find_participant(cluster.consumer, cluster.origin_id);
    const bool origin_retired       = after == nullptr;
    const int  origin_disappeared   = kind_count(obs, cluster.origin_id, graph::change_kind::disappeared);
    const bool arbiter_saw_origin   = arbiter_saw(obs, cluster.origin_id);
    const bool arbiter_saw_relay    = arbiter_saw(obs, cluster.relay_id);
    INFO("relay_session_gone=" << relay_session_gone << " origin_retired=" << origin_retired
         << " origin_disappeared_deltas=" << origin_disappeared << " arbiter_saw_origin=" << arbiter_saw_origin
         << " arbiter_saw_relay=" << arbiter_saw_relay);

    // Ground truth, pinned to the observed behavior: the consumer notices the relay's teardown edge and
    // PROMPTLY RETIRES the origin's via-relay row, emitting change_kind::disappeared — byte-identical to
    // a genuinely-dead peer. The via-only origin never receives an arbiter verdict; only the relay (a
    // direct peer) can.
    REQUIRE(relay_session_gone);
    REQUIRE(origin_retired);
    REQUIRE(origin_disappeared == 1);
    REQUIRE_FALSE(arbiter_saw_origin);

    std::optional<bool> ok;
    plexus::call_options copts;
    copts.deadline = std::chrono::milliseconds(300);
    plexus::caller<> call{cluster.consumer, k_procedure};
    call.call(as_bytes(std::string{"ping"}), copts, [&](plexus::expected<plexus::reply, std::error_code> r) { ok = r.has_value(); });
    cluster.pump([&] { return ok.has_value(); });

    REQUIRE(ok.has_value());
    REQUIRE_FALSE(*ok);          // no live via-session — the request cannot complete
    REQUIRE(served.empty());     // the origin's handler never ran through the dead relay
    REQUIRE(cluster.consumer.router().forward_rpc_dropped_count() >= drops_before);
}

// The recovery capability the liveliness half's revival arm needs: after the relay dies (the origin's
// via-relay row retires to disappeared), a fresh relay under the SAME identity re-establishes BOTH legs
// and the relayed path's control + demand plane comes back — the origin resolves under the same node_id
// (no identity churn), reachable via the relay, and the consumer's remembered demand re-propagates all
// the way up onto the origin. The DATA-plane delivery of a fresh publish does NOT yet recover, and this
// smoke test deliberately stops short of asserting it: because recovery keeps the relay identity, the
// consumer's per-(origin, relay-identity) forward dedup window survives the relay's death, while the
// revived relay's splice restarts its forwarding sequence at 0 — so the first re-forwarded publish is
// dropped as a duplicate against the stale window. Resetting that window on relay return is the recovery
// derivation's job (owned by a later plan), not this fixture's.
void run_revive_relay_smoke()
{
    cold_cluster cluster;

    std::optional<plexus::publisher<>> pub;
    pub.emplace(*cluster.origin, k_topic, plexus::topic_qos{}, /*emit_source_identity=*/true);

    delivery got;
    cluster.bring_up_serial();
    cluster.pump([&] { return cluster.relay->count_publishers(std::string{k_topic}) >= 1; });
    cluster.bring_up_tcp();
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });

    plexus::subscriber<> sub{cluster.consumer, k_topic,
                             [&](std::span<const std::byte> b, const plexus::io::message_info &info)
                             {
                                 got.body = to_string(b);
                                 if(info.source_identity)
                                     got.source = info.source_identity->node_id();
                             }};
    cluster.pump([&] { return cluster.origin->count_subscribers(std::string{k_topic}) >= 1; });
    pub->publish(as_bytes(std::string{"24.5C"}));
    cluster.pump([&] { return !got.body.empty(); });
    REQUIRE(got.body == "24.5C");
    REQUIRE(got.source == cluster.origin_id);

    cluster.kill_relay();
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) == nullptr; });
    REQUIRE(find_participant(cluster.consumer, cluster.origin_id) == nullptr);

    cluster.revive_relay();
    cluster.pump([&] { return find_participant(cluster.consumer, cluster.origin_id) != nullptr; });
    const auto *back = find_participant(cluster.consumer, cluster.origin_id);
    REQUIRE(back != nullptr);
    REQUIRE(back->id == cluster.origin_id);       // same identity across the kill->revive cycle
    REQUIRE(back->reach.via == cluster.relay_id); // reachable again via the revived relay

    // The consumer's remembered demand re-propagates all the way up onto the origin over the revived
    // legs — the relayed path's control + demand plane is live again.
    cluster.pump([&] { return cluster.origin->count_subscribers(std::string{k_topic}) >= 1; });
    REQUIRE(cluster.origin->count_subscribers(std::string{k_topic}) >= 1);
    REQUIRE(cluster.relay->count_publishers(std::string{k_topic}) >= 1);

    pub.reset();
    cluster.kill_origin();
}

}

TEST_CASE("a composed relay discloses it offers a relayed path and its reported-origin count moves with a real lift",
          "[integration][serial][relay][e2e][self_check]")
{
    run_relay_self_check_discloses_offering();
}

TEST_CASE("a non-relay leaf discloses routes transiting a relay after a real peer_report ingest",
          "[integration][serial][relay][e2e][self_check]")
{
    run_leaf_self_check_discloses_routes_via_relay();
}

TEST_CASE("probe: killing the whole relay retires the origin's via-relay row and the arbiter stays silent for it",
          "[integration][serial][relay][e2e][kill_relay][probe]")
{
    for(int run = 0; run < 2; ++run)
        run_kill_relay_probe();
}

TEST_CASE("a revived relay under the same identity re-establishes both legs and the relayed path recovers, over cold runs",
          "[integration][serial][relay][e2e][revive]")
{
    for(int run = 0; run < 2; ++run)
        run_revive_relay_smoke();
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

TEST_CASE("a never consumer enumerates a relayed origin but never consumes its relayed publish, over cold runs",
          "[integration][serial][relay][e2e][route_usage]")
{
    for(int run = 0; run < 2; ++run)
        run_never_denies_relayed_pubsub();
}

TEST_CASE("a never consumer's via-relay call finds no relayed provider, over cold runs",
          "[integration][serial][relay][e2e][route_usage]")
{
    for(int run = 0; run < 2; ++run)
        run_never_call_finds_no_relayed_provider();
}

TEST_CASE("an allow_relayed consumer reaches the relayed origin a never consumer refuses, over cold runs",
          "[integration][serial][relay][e2e][route_usage]")
{
    for(int run = 0; run < 2; ++run)
        run_allow_relayed_reaches_relayed_origin();
}

TEST_CASE("a declining origin is never announced over the relay while a non-declining second origin is, over cold runs",
          "[integration][serial][relay][e2e][decline]")
{
    for(int run = 0; run < 2; ++run)
        run_origin_decline_suppresses_announcement();
}

TEST_CASE("an origin flipping its decline mid-session is withdrawn from the consumer downstream, over cold runs",
          "[integration][serial][relay][e2e][decline]")
{
    for(int run = 0; run < 2; ++run)
        run_origin_decline_flip_withdraws_downstream();
}

TEST_CASE("a declining consumer is offered no relayed origin the relay otherwise reports, over cold runs",
          "[integration][serial][relay][e2e][decline]")
{
    for(int run = 0; run < 2; ++run)
        run_consumer_decline_receives_no_offer();
}

TEST_CASE("a dual-homed consumer's relayed delivery is superseded by a live direct session and re-arms on its loss, exactly-once per seq",
          "[integration][relay][e2e][switchover]")
{
    for(int run = 0; run < 2; ++run)
        run_switchover_relay_first();
}

TEST_CASE("a direct session live before the origin is reported suppresses the later relayed twin at ingest, no double-delivery",
          "[integration][relay][e2e][switchover]")
{
    for(int run = 0; run < 2; ++run)
        run_switchover_direct_first();
}

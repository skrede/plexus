// Withdrawal and TTL retirement of a transitive reported entry, on the inproc virtual clock: a fresh
// re-report over a live reporter refreshes the row; a withdrawal-flagged report retires it AND the
// origin's topic edges immediately; stopping the reports lets the existing awareness sweep age the
// row out and retire its topics; and a retired origin re-appears under the SAME node_id — a
// reachability row retires, the identity never churns.

#include "support/graph_change_inproc.h"

#include "plexus/discovery/universe.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/peer_report.h"
#include "plexus/wire/topic_declaration.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using namespace plexus::testing::graph_change_fixture;
using plexus::node_id;
namespace wire = plexus::wire;

namespace {

constexpr auto k_ttl = std::chrono::milliseconds(1000);

wire::peer_report make_report(const node_id &origin, std::uint8_t hop, std::uint16_t seq, std::uint8_t flags = wire::k_peer_report_consent_flag)
{
    wire::peer_report pr;
    pr.origin          = origin;
    pr.origin_universe = plexus::discovery::k_default_universe;
    pr.hop             = hop;
    pr.seq             = seq;
    pr.flags           = flags | wire::k_peer_report_topics_flag;
    pr.topics          = {wire::topic_declaration{.topic_hash = 7, .type_id = 9, .fqn = "sensor/imu", .type_name = "geometry/Pose", .state = wire::type_state::declared}};
    return pr;
}

int deltas_for(const recording_graph_observer &obs, const node_id &who, plexus::graph::change_kind kind)
{
    int n = 0;
    for(const auto &d : obs.deltas)
        if(d.who == who && d.kind == kind)
            ++n;
    return n;
}

struct aging_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t transport{ex, bus};
    plexus::log::null_logger sink;
    engine eng;
    recording_graph_observer obs;
    node_id self{make_id(0xA1)};
    node_id origin{make_id(0xC3)};
    node_id reporter{make_id(0xB2)};

    aging_node()
            : eng(transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, live_opts())
    {
        eng.listen(ep_for("node-a"));
        obs.graph_opt_in = true;
        eng.add_observer(obs);
    }

    static plexus::io::liveliness_options live_opts()
    {
        plexus::io::liveliness_options live{};
        live.awareness_ttl = std::chrono::nanoseconds(k_ttl);
        return live;
    }

    void ingest(const wire::peer_report &pr)
    {
        eng.ingest_peer_report(reporter, pr);
        ex.drain();
    }

    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }

    std::size_t candidate_count(const node_id &id) const
    {
        return eng.known().candidates(id).size();
    }

    int origin_topic_edges() const
    {
        int n = 0;
        eng.topic_table().for_each([&](const plexus::graph::topic_record &rec) { n += static_cast<int>(rec.node == origin); });
        return n;
    }
};

}

TEST_CASE("peer_report withdrawal: a re-report refreshes a live reported row", "[graph][peer_report][withdrawal]")
{
    manual_clock::reset();
    aging_node n;
    n.ingest(make_report(n.origin, 1, 1));
    REQUIRE(n.candidate_count(n.origin) == 1);
    REQUIRE(n.origin_topic_edges() == 1);

    // A re-report just under a TTL keeps the row alive across a sweep that would otherwise age it.
    n.advance(k_ttl - std::chrono::milliseconds(100));
    n.ingest(make_report(n.origin, 1, 2));
    n.advance(k_ttl - std::chrono::milliseconds(100));

    REQUIRE(n.candidate_count(n.origin) == 1);
    REQUIRE(n.origin_topic_edges() == 1);
}

TEST_CASE("peer_report withdrawal: a goodbye-flagged report retires the row and the origin topics", "[graph][peer_report][withdrawal]")
{
    manual_clock::reset();
    aging_node n;
    n.ingest(make_report(n.origin, 1, 1));
    REQUIRE(n.candidate_count(n.origin) == 1);

    n.ingest(make_report(n.origin, 1, 2, wire::k_peer_report_withdrawal_flag));

    REQUIRE(n.candidate_count(n.origin) == 0);
    REQUIRE(n.origin_topic_edges() == 0);
    REQUIRE(deltas_for(n.obs, n.origin, plexus::graph::change_kind::disappeared) == 1);
}

TEST_CASE("peer_report withdrawal: stopping the reports ages the row out on the existing sweep", "[graph][peer_report][withdrawal]")
{
    manual_clock::reset();
    aging_node n;
    n.ingest(make_report(n.origin, 1, 1));
    REQUIRE(n.candidate_count(n.origin) == 1);
    REQUIRE(n.origin_topic_edges() == 1);

    // No further reports: a live path to origin is the only refresh, so the row ages out.
    n.advance(k_ttl * 2);

    REQUIRE(n.candidate_count(n.origin) == 0);
    REQUIRE(n.origin_topic_edges() == 0);
    REQUIRE(deltas_for(n.obs, n.origin, plexus::graph::change_kind::disappeared) == 1);
}

TEST_CASE("peer_report withdrawal: a replayed stale withdrawal cannot retire a live row", "[graph][peer_report][withdrawal]")
{
    manual_clock::reset();
    aging_node n;
    n.ingest(make_report(n.origin, 1, 5));
    REQUIRE(n.candidate_count(n.origin) == 1);

    // A withdrawal whose seq the row's dedup window already admitted (a datagram-reorder replay, or a
    // hostile relay alternating assert/withdraw) is stale: it must NOT destroy the window and retire.
    n.ingest(make_report(n.origin, 1, 5, wire::k_peer_report_withdrawal_flag));
    REQUIRE(n.candidate_count(n.origin) == 1);
    REQUIRE(n.origin_topic_edges() == 1);
    REQUIRE(deltas_for(n.obs, n.origin, plexus::graph::change_kind::disappeared) == 0);

    // A withdrawal with a fresh seq is honored.
    n.ingest(make_report(n.origin, 1, 6, wire::k_peer_report_withdrawal_flag));
    REQUIRE(n.candidate_count(n.origin) == 0);
    REQUIRE(deltas_for(n.obs, n.origin, plexus::graph::change_kind::disappeared) == 1);
}

TEST_CASE("peer_report withdrawal: a retired origin re-appears under the same node_id", "[graph][peer_report][withdrawal]")
{
    manual_clock::reset();
    aging_node n;
    n.ingest(make_report(n.origin, 1, 1));
    n.ingest(make_report(n.origin, 1, 2, wire::k_peer_report_withdrawal_flag));
    REQUIRE(n.candidate_count(n.origin) == 0);
    REQUIRE(deltas_for(n.obs, n.origin, plexus::graph::change_kind::disappeared) == 1);

    const std::size_t before = n.obs.deltas.size();
    n.ingest(make_report(n.origin, 1, 3));

    // The SAME origin id re-appears with the same reporter via — a reachability row returns, the
    // identity never churned — and a fresh appeared followed the retirement.
    REQUIRE(n.candidate_count(n.origin) == 1);
    REQUIRE(n.eng.known().candidates(n.origin)[0].reach.via == n.reporter);
    int appeared_after = 0;
    for(std::size_t i = before; i < n.obs.deltas.size(); ++i)
        appeared_after += static_cast<int>(n.obs.deltas[i].who == n.origin && n.obs.deltas[i].kind == plexus::graph::change_kind::appeared);
    REQUIRE(appeared_after >= 1);
}

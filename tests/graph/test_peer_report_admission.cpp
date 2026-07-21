// The peer_report receive gate chain, driven at the engine seam over the inproc virtual clock: a
// valid in-universe report admits a via-only reported candidate and bumps the graph appeared; a
// foreign-ORIGIN-universe report, a self-origin report, a duplicate (origin, seq), and an
// over-budget hop are each no-ops that leave the direct table and the graph untouched. A reported
// candidate never dials and never enters the direct-peer awareness table.

#include "support/graph_change_inproc.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/peer_report.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using namespace plexus::testing::graph_change_fixture;
using plexus::node_id;
namespace wire = plexus::wire;

namespace {

wire::peer_report make_report(const node_id &origin, std::uint32_t universe, std::uint8_t hop, std::uint16_t seq, std::uint8_t flags = wire::k_peer_report_consent_flag)
{
    wire::peer_report pr;
    pr.origin          = origin;
    pr.origin_universe = universe;
    pr.hop             = hop;
    pr.seq             = seq;
    pr.flags           = flags;
    return pr;
}

int appeared_for(const recording_graph_observer &obs, const node_id &who)
{
    int n = 0;
    for(const auto &d : obs.deltas)
        if(d.who == who && d.kind == plexus::graph::change_kind::appeared)
            ++n;
    return n;
}

// The direct-awareness enumeration (known().for_each yields the direct endpoint only): a via-only
// reported candidate must never surface here — it is reachability hearsay, not a direct participant.
bool direct_has(const engine &eng, const node_id &id)
{
    bool found = false;
    eng.known().for_each([&](const node_id &seen, const plexus::io::endpoint &) { found = found || seen == id; });
    return found;
}

struct fixture
{
    one_node node;
    recording_graph_observer obs;
    node_id origin{make_id(0xC3)};
    node_id reporter{make_id(0xB2)};

    fixture()
    {
        manual_clock::reset();
        obs.graph_opt_in = true;
        node.eng.add_observer(obs);
    }

    void ingest(const wire::peer_report &pr)
    {
        node.eng.ingest_peer_report(reporter, pr);
        node.drain();
    }

    std::size_t candidate_count(const node_id &id) const
    {
        return node.eng.known().candidates(id).size();
    }
};

}

TEST_CASE("peer_report admission: a valid in-universe report admits a via-only reported candidate", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 1, 1));

    REQUIRE(f.candidate_count(f.origin) == 1);
    const auto cand = f.node.eng.known().candidates(f.origin)[0];
    REQUIRE_FALSE(cand.is_direct());
    REQUIRE(cand.reach.via == f.reporter);
    REQUIRE(cand.origin.how == plexus::graph::observation::reported);
    REQUIRE(cand.origin.reporter == f.reporter);
    REQUIRE(appeared_for(f.obs, f.origin) == 1);

    // Reported is via-only: it neither surfaces as a direct participant nor opens a session (no dial).
    REQUIRE_FALSE(direct_has(f.node.eng, f.origin));
    REQUIRE_FALSE(f.node.eng.is_connected(f.origin));
    REQUIRE_FALSE(f.node.eng.has_session(f.origin));
}

TEST_CASE("peer_report admission: a foreign-origin-universe report is a zero-awareness no-op", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe + 1u, 1, 1));

    REQUIRE(f.candidate_count(f.origin) == 0);
    REQUIRE(appeared_for(f.obs, f.origin) == 0);
    REQUIRE_FALSE(direct_has(f.node.eng, f.origin));
}

TEST_CASE("peer_report admission: a self-origin report is dropped", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.node.self, plexus::discovery::k_default_universe, 1, 1));

    REQUIRE(f.candidate_count(f.node.self) == 0);
    REQUIRE(appeared_for(f.obs, f.node.self) == 0);
}

TEST_CASE("peer_report admission: a duplicate (origin, seq) is a no-op", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 1, 7));
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 1, 7));

    REQUIRE(f.candidate_count(f.origin) == 1);
    REQUIRE(appeared_for(f.obs, f.origin) == 1);
}

TEST_CASE("peer_report admission: an over-budget hop is dropped", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 2, 1));

    REQUIRE(f.candidate_count(f.origin) == 0);
    REQUIRE(appeared_for(f.obs, f.origin) == 0);
}

TEST_CASE("peer_report admission: a fresh re-report refreshes without a second appeared", "[graph][peer_report][admission]")
{
    fixture f;
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 1, 1));
    f.ingest(make_report(f.origin, plexus::discovery::k_default_universe, 1, 2));

    REQUIRE(f.candidate_count(f.origin) == 1);
    REQUIRE(appeared_for(f.obs, f.origin) == 1);
}

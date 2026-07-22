// The host {who, appeared|disappeared} payload oracle for the graph-change observer (GRAPH-07). On the
// heap profile the additive edge-log is compiled in, but maintained only while a registered observer
// declares interest through observes_graph(): an opt-in observer drains the exact {who, kind} deltas
// for a sequence of appears/disappears, while a non-opt-in observer leaves the log empty (inert when
// unused). These host test binaries exercise only the heap engine — the edge-log is structurally
// absent on bounded<>. RED until the payload edge-log lands.

#include "support/graph_change_inproc.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/peer_report.h"
#include "plexus/wire/topic_declaration.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using namespace plexus::testing::graph_change_fixture;
using plexus::graph::change_kind;
using plexus::graph::reachability;

TEST_CASE("graph change payload: an observes_graph observer drains the exact {who, kind} deltas for a "
          "sequence of appears and disappears",
          "[graph][change][payload]")
{
    one_node net;
    recording_graph_observer rec;
    rec.graph_opt_in = true; // observes_graph() == true — the edge-log is maintained for this observer
    net.eng.add_observer(rec);

    const auto id_b = make_id(0xB2);
    const auto id_c = make_id(0xC3);
    net.eng.note_peer(id_b, ep_for("node-b")); // appeared
    net.eng.note_peer(id_c, ep_for("node-c")); // appeared
    net.eng.forget(id_b);                       // disappeared
    net.drain();

    REQUIRE(rec.deltas.size() == 3);
    REQUIRE(rec.deltas[0].who == id_b);
    REQUIRE(rec.deltas[0].kind == change_kind::appeared);
    REQUIRE(rec.deltas[1].who == id_c);
    REQUIRE(rec.deltas[1].kind == change_kind::appeared);
    REQUIRE(rec.deltas[2].who == id_b);
    REQUIRE(rec.deltas[2].kind == change_kind::disappeared);
}

TEST_CASE("graph change payload: without observes_graph the edge-log stays empty (inert when unused), "
          "yet the coarse edge still fires",
          "[graph][change][payload]")
{
    one_node net;
    recording_graph_observer rec; // graph_opt_in defaults false — no edge-log maintained
    net.eng.add_observer(rec);

    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.eng.note_peer(make_id(0xC3), ep_for("node-c"));
    net.eng.forget(make_id(0xB2));
    net.drain();

    REQUIRE(rec.deltas.empty());     // no payload maintained for a non-opt-in observer
    REQUIRE(rec.changed_fires == 1); // the coarse coalesced edge still fires
}

namespace {

namespace wire = plexus::wire;

wire::peer_report make_report(const plexus::node_id &origin, std::uint16_t seq)
{
    wire::peer_report pr;
    pr.origin          = origin;
    pr.origin_universe = plexus::discovery::k_default_universe;
    pr.hop             = 1;
    pr.seq             = seq;
    pr.flags           = wire::k_peer_report_consent_flag | wire::k_peer_report_topics_flag;
    pr.topics          = {wire::topic_declaration{.topic_hash = 7, .type_id = 9, .fqn = "sensor/imu", .type_name = "geometry/Pose", .state = wire::type_state::declared}};
    return pr;
}

int deltas_for(const recording_graph_observer &obs, const plexus::node_id &who, change_kind kind)
{
    int n = 0;
    for(const auto &d : obs.deltas)
        n += static_cast<int>(d.who == who && d.kind == kind);
    return n;
}

// A downstream D holding a live session to a relay B that reports a third-party origin O. Tearing the
// D<->B session down degrades O to UNREACHABLE-NOT-DEAD (the relay-death caller marks in place); a
// subsequent seq-fresh re-report about O over the (dead-then-re-fed) reporter flips it back to reachable.
// Reports are injected directly with controlled seqs so the recovery flip and its stale-report rejection
// are deterministic without a redial dance.
struct relay_downstream
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t tr_d{ex, bus};
    transport_t tr_b{ex, bus};
    plexus::log::null_logger sink;
    engine d;
    engine b;
    recording_graph_observer obs;
    plexus::node_id id_b{make_id(0xB2)};
    plexus::node_id origin{make_id(0xC3)};

    relay_downstream()
            : d(tr_d, ex, make_cfg(0xD4), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , b(tr_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        d.listen(ep_for("node-d"));
        b.listen(ep_for("node-b"));
        obs.graph_opt_in = true;
        d.add_observer(obs);
        d.note_peer(id_b, ep_for("node-b"));
        d.reach(id_b);
        ex.drain();
    }

    void ingest(std::uint16_t seq)
    {
        d.ingest_peer_report(id_b, make_report(origin, seq));
        ex.drain();
    }

    void drop_relay_session()
    {
        d.registry().for_each_connected([&](const plexus::node_id &, auto &s) { if(s.peer_identity() == id_b) s.tear_down(); });
        ex.drain();
    }

    reachability status() const
    {
        for(const auto &c : d.known().candidates(origin))
            if(!c.is_direct() && c.reach.via == id_b)
                return c.origin.reach_status;
        return reachability::reachable;
    }
};

}

TEST_CASE("graph change payload: a returning relay flips an unreachable origin back to reachable — one "
          "reachable delta, never a disappeared+appeared churn, and a stale report flips nothing",
          "[graph][change][payload][reachable][recover]")
{
    manual_clock::reset();
    relay_downstream net;

    net.ingest(5); // O appeared via B (a first sighting: its row and its topic edge)
    REQUIRE(net.status() == reachability::reachable);
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::appeared) >= 1);

    net.drop_relay_session(); // B's session dies: O degrades to UNREACHABLE-NOT-DEAD, retained in place
    REQUIRE(net.status() == reachability::unreachable);
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::unreachable) == 1);

    // A stale replay from the dead incarnation (a seq the row's window already admitted) is rejected by
    // the existing seq-freshness discipline before it can reach the recovery flip: nothing changes.
    net.ingest(5);
    REQUIRE(net.status() == reachability::unreachable);
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::reachable) == 0);

    // The relay returns and re-reports with a fresh seq: O recovers to reachable under the SAME identity.
    // The recovery is a single reachable delta and NEVER a disappeared+appeared pair — no new appeared
    // crosses (a re-appear would be identity churn at the notification level) and disappeared never did.
    const int appeared_before = deltas_for(net.obs, net.origin, change_kind::appeared);
    net.ingest(6);
    REQUIRE(net.status() == reachability::reachable);
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::reachable) == 1);
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::appeared) == appeared_before); // no re-appear
    REQUIRE(deltas_for(net.obs, net.origin, change_kind::disappeared) == 0);            // never disappeared
    REQUIRE(net.d.known().candidates(net.origin)[0].reach.via == net.id_b);            // same via, same node_id throughout
}

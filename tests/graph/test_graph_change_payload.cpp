// The host {who, appeared|disappeared} payload oracle for the graph-change observer (GRAPH-07). On the
// heap profile the additive edge-log is compiled in, but maintained only while a registered observer
// declares interest through observes_graph(): an opt-in observer drains the exact {who, kind} deltas
// for a sequence of appears/disappears, while a non-opt-in observer leaves the log empty (inert when
// unused). These host test binaries exercise only the heap engine — the edge-log is structurally
// absent on bounded<>. RED until the payload edge-log lands.

#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

using namespace plexus::testing::graph_change_fixture;
using plexus::graph::change_kind;

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

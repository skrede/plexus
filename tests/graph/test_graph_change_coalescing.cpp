// The coalescing / idempotency oracle for the coarse graph-change observer (SC#1, GRAPH-05). A
// recording graph observer on a real inproc engine drives the graph mutation seams; every assertion
// is made after drain(). The suite pins the level-triggered single-wakeup coalescing (a burst of real
// changes collapses to ONE fire at the final generation, the 1->2->1 case losing no transition), the
// idempotent no-fire (a re-announce / duplicate declare neither fires nor bumps the generation), and
// the D-04 edge classes each firing the coarse signal once. RED until the coalescing engine lands.

#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

#include <optional>

using namespace plexus::testing::graph_change_fixture;
using plexus::graph::topic_role;

TEST_CASE("graph change coalescing: a burst of distinct mutations before one drain fires the coarse "
          "signal exactly once at the final generation",
          "[graph][change][coalescing]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    const auto before = net.eng.graph_generation();
    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.eng.note_peer(make_id(0xC3), ep_for("node-c"));
    net.eng.note_local_topic("topic/a", "", std::nullopt, topic_role::publisher);
    net.eng.note_local_topic("topic/b", "", std::nullopt, topic_role::publisher);
    net.drain();

    REQUIRE(rec.changed_fires == 1);
    REQUIRE(rec.last_generation == net.eng.graph_generation());
    REQUIRE(net.eng.graph_generation() > before);
}

TEST_CASE("graph change coalescing: two rapid changes before one drain collapse to a single wakeup at "
          "the final generation (no lost 1->2->1 transition)",
          "[graph][change][coalescing]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    // Appear B, appear C, then disappear B — three transitions before one drain. The consumer must
    // see ONE wakeup carrying the final generation and re-snapshot to truth, never a lost edge.
    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.eng.note_peer(make_id(0xC3), ep_for("node-c"));
    net.eng.forget(make_id(0xB2));
    net.drain();

    REQUIRE(rec.changed_fires == 1);
    REQUIRE(rec.last_generation == net.eng.graph_generation());
}

TEST_CASE("graph change coalescing: an idempotent re-announce and duplicate declare neither fire nor "
          "bump the generation",
          "[graph][change][coalescing]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.eng.note_local_topic("topic/a", "pkg/T", 1u, topic_role::publisher);
    net.drain();
    REQUIRE(rec.changed_fires == 1);
    const auto settled = net.eng.graph_generation();

    // Re-announce the SAME (id, endpoint) and re-declare the SAME (topic, role, type): nothing
    // changes, so no coarse edge fires and the generation does not move (the D-04 no-fire case).
    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.eng.note_local_topic("topic/a", "pkg/T", 1u, topic_role::publisher);
    net.drain();
    REQUIRE(rec.changed_fires == 1);
    REQUIRE(net.eng.graph_generation() == settled);
}

TEST_CASE("graph change coalescing: each D-04 edge class fires the coarse signal once and bumps the "
          "generation",
          "[graph][change][coalescing]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    int expected = 0;
    auto step    = [&](auto mutate) {
        const auto before = net.eng.graph_generation();
        mutate();
        net.drain();
        REQUIRE(rec.changed_fires == ++expected);
        REQUIRE(net.eng.graph_generation() > before);
    };

    step([&] { net.eng.note_peer(make_id(0xB2), ep_for("node-b")); });                              // participant add
    step([&] { net.eng.note_local_topic("topic/a", "", std::nullopt, topic_role::publisher); });    // topic add
    step([&] { net.eng.note_local_topic("topic/a", "", std::nullopt, topic_role::subscriber); });   // pub/sub count change
    step([&] { net.eng.note_local_topic("topic/a", "pkg/T", 1u, topic_role::publisher); });         // type absent->known
    step([&] { net.eng.note_peer(make_id(0xB2), ep_for("node-b2")); });                             // reachability change
    step([&] { net.eng.forget_local_topic("topic/a", topic_role::subscriber); });                   // topic remove
    step([&] { net.eng.forget(make_id(0xB2)); });                                                   // participant remove
}

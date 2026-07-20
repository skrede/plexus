// The posted-not-synchronous teardown oracle for the coarse graph-change observer (SC#2). A recording
// graph observer on a real inproc engine proves the coarse edge is POSTED, never emitted synchronously
// from the mutation site: no on_graph_changed is observed WITHOUT an intervening drain(), an observer
// removed BEFORE the drain receives no in-flight wakeup (the remove-before-teardown discipline that
// keeps a posted value from firing into freed state), and after remove_observer a later mutation +
// drain fires nothing into it. Runs under asan/ubsan at the phase gate. RED until the engine lands.

#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

using namespace plexus::testing::graph_change_fixture;

TEST_CASE("graph change teardown: the coarse signal is posted, never synchronous — no fire without an "
          "intervening drain",
          "[graph][change][teardown]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    REQUIRE(rec.changed_fires == 0); // the edge is posted, not emitted from the mutation site
    net.drain();
    REQUIRE(rec.changed_fires == 1);
}

TEST_CASE("graph change teardown: an observer removed before the drain receives no in-flight wakeup "
          "(nothing fires into a torn-down listener)",
          "[graph][change][teardown]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    net.eng.note_peer(make_id(0xB2), ep_for("node-b")); // posts a wakeup
    net.eng.remove_observer(rec);                        // handle teardown BEFORE the drain
    net.drain();
    REQUIRE(rec.changed_fires == 0);
}

TEST_CASE("graph change teardown: after remove_observer a later mutation + drain fires nothing into it",
          "[graph][change][teardown]")
{
    one_node net;
    recording_graph_observer rec;
    net.eng.add_observer(rec);

    net.eng.note_peer(make_id(0xB2), ep_for("node-b"));
    net.drain();
    REQUIRE(rec.changed_fires == 1);

    net.eng.remove_observer(rec);
    net.eng.note_peer(make_id(0xC3), ep_for("node-c"));
    net.drain();
    REQUIRE(rec.changed_fires == 1); // no further fire into the removed observer
}

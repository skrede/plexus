// The zero-allocation gate for the coarse graph-change path (SC#4). This TU OWNS the single
// replaceable operator new for its binary (support/alloc_counter.h), so it is its own executable and
// runs RUN_SERIAL. A warmed mutation->drain->on_graph_changed loop over a coarse-only observer
// (observes_graph() == false, so no host edge-log) allocates ZERO: a real reachability change bumps
// the generation and posts exactly one wakeup closure, which must fit move_only_function's SBO. The
// gate is exercised over both the heap engine and the bounded-shaped engine, so the alloc-free claim
// covers the coarse path on the MCU profile. RED until the coalescing engine lands in Wave 2.

#include "support/alloc_counter.h"
#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

using namespace plexus::testing::graph_change_fixture;

namespace {

// Warm the peer entry + any one-time lazy allocation, snapshot the counter, then run a steady loop
// that toggles the SAME peer's endpoint in place (updates, never grows the table). Each toggle is a
// real reachability change: it bumps the generation, posts one coarse wakeup, and fires the coarse
// edge. The delta must be zero.
template<typename Node>
void assert_coarse_path_allocates_nothing()
{
    Node net;
    recording_graph_observer rec; // coarse-only: no observes_graph opt-in, so no edge-log
    net.eng.add_observer(rec);

    const auto id_b = make_id(0xB2);
    net.eng.note_peer(id_b, ep_for("node-b"));
    net.drain();

    plexus::testing::reset_alloc_count();
    constexpr int k_iterations = 1000;
    for(int i = 0; i < k_iterations; ++i)
    {
        net.eng.note_peer(id_b, ep_for((i & 1) ? "node-b" : "node-b2"));
        net.drain();
    }

    REQUIRE(rec.changed_fires >= k_iterations);        // the coarse path actually fired every turn
    REQUIRE(plexus::testing::alloc_count() == 0);      // and allocated nothing on the steady path
}

}

TEST_CASE("graph change alloc: the warmed coarse mutation->drain->fire loop allocates nothing on the "
          "heap engine",
          "[integration][graph][change][alloc]")
{
    assert_coarse_path_allocates_nothing<one_node>();
}

TEST_CASE("graph change alloc: the coarse path allocates nothing on the bounded engine "
          "(null_graph_change_log by construction)",
          "[integration][graph][change][alloc]")
{
    assert_coarse_path_allocates_nothing<bounded_one_node>();
}

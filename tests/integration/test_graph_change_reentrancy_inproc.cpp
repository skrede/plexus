// The destroy-peer-mid-re-dial non-abort integration oracle (SC#3). A graph observer is registered on
// a real inproc engine, a peer is brought up and then a REAL transport drop arms A's redial; while the
// redial is in flight the peer is dropped (forget + session teardown). The posted graph wakeup must
// drain over a surviving snapshot and never re-enter the freed slot_block — the rmw_fastrtps #496
// analogue. The suite drives to quiescence via drain() and asserts no abort, a stable engine, and a
// post-drain awareness snapshot reflecting the removal. This TU is the one built under the asan/ubsan
// no-recover config at the phase gate. RED until the coalescing engine lands in Wave 2.

#include "support/graph_change_inproc.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

using namespace plexus::testing::graph_change_fixture;

namespace {

// The post-drain awareness snapshot: how many entries the engine's known-peer sweep still holds for
// this id (participants() reduces over the same table). Zero proves the removal is reflected.
std::size_t known_count(engine &eng, const plexus::node_id &id)
{
    std::size_t n = 0;
    eng.known().for_each([&](const plexus::node_id &k, const plexus::io::endpoint &) {
        if(k == id)
            ++n;
    });
    return n;
}

}

TEST_CASE("graph change reentrancy: dropping a peer mid-redial with a graph observer registered reaches "
          "quiescence without abort and reflects the removal",
          "[integration][graph][change][reentrancy]")
{
    constexpr int k_iterations = 50;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;
        recording_graph_observer rec;
        net.a.add_observer(rec);

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));

        // A REAL transport drop: close B's accepted end so A observes on_error and arms its redial
        // backoff — the dialing session is mid-rebuild.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();

        // Drop the peer WHILE the redial is in flight: forget its awareness and tear down A's dialing
        // session. The posted graph wakeup must drain over a snapshot that outlives the teardown,
        // never re-entering the freed slot_block (SC#3).
        net.a.forget(net.id_b);
        if(auto *s = net.a.session_for(net.id_b))
            s->tear_down();

        net.advance(k_ceiling); // drive the redial timers to their bound and drain to quiescence
        net.drive();

        REQUIRE_FALSE(net.a.is_connected(net.id_b)); // the engine settled, no resurrection
        REQUIRE(known_count(net.a, net.id_b) == 0);  // the awareness snapshot reflects the removal
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

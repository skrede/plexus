// The refresh/lifetime story of a relayed origin, driven end to end over the inproc virtual clock: a
// relay periodically re-asserts every held report on its heartbeat cadence, so a still-live-but-idle
// origin survives past the downstream's awareness_ttl instead of silently vanishing; and when the
// downstream's session to the relay dies, the origins reachable only via that relay retire at once
// (the dead relay can no longer withdraw them) rather than lingering as phantoms until the sweep.

#include "support/graph_change_inproc.h"

#include "plexus/io/peer_report_emitter.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/graph/std_map_topic_storage.h"
#include "plexus/graph/vector_graph_change_log.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using namespace plexus::testing::graph_change_fixture;
using plexus::node_id;

namespace {

using relay_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, plexus::io::std_map_peer_storage, plexus::graph::std_map_topic_storage,
                                                plexus::io::default_liveliness_storage, plexus::graph::vector_graph_change_log, plexus::io::peer_report_emitter>;

plexus::io::liveliness_options short_ttl()
{
    plexus::io::liveliness_options live{};
    live.awareness_ttl = std::chrono::milliseconds(500);
    return live;
}

// A relay R dialing an origin O and a downstream D on one bus; R carries the real emitter. All three
// run a short awareness TTL so the test crosses it quickly on the virtual clock.
struct relay_net
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t tr_r{ex, bus};
    transport_t tr_o{ex, bus};
    transport_t tr_d{ex, bus};
    plexus::log::null_logger sink;

    relay_engine r;
    engine o;
    engine d;

    node_id id_o{make_id(0xC3)};
    node_id id_d{make_id(0xD4)};

    relay_net()
            : r(tr_r, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, short_ttl())
            , o(tr_o, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, short_ttl())
            , d(tr_d, ex, make_cfg(0xD4), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, short_ttl())
    {
        r.listen(ep_for("node-r"));
        o.listen(ep_for("node-o"));
        d.listen(ep_for("node-d"));
    }

    void drive()
    {
        ex.drain();
    }

    void advance(std::chrono::nanoseconds t)
    {
        manual_clock::advance(t);
        drive();
    }

    void establish()
    {
        r.note_peer(id_o, ep_for("node-o"));
        r.reach(id_o);
        r.note_peer(id_d, ep_for("node-d"));
        r.reach(id_d);
        drive();
    }

    std::size_t downstream_sees_origin() const
    {
        return d.known().candidates(id_o).size();
    }

    // The reachability the downstream's only (via-relay) row for O currently carries.
    plexus::graph::reachability origin_reach_status() const
    {
        for(const auto &c : d.known().candidates(id_o))
            if(!c.is_direct() && c.reach.via == id_r)
                return c.origin.reach_status;
        return plexus::graph::reachability::reachable;
    }

    node_id id_r{make_id(0xA1)};
};

}

TEST_CASE("relay refresh: a live-but-idle reported origin survives past awareness_ttl", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    relay_net net;
    net.establish();
    REQUIRE(net.downstream_sees_origin() == 1);

    // No topic change and no session churn: only the relay's periodic re-assert keeps the downstream
    // row alive across four awareness_ttl windows while the origin's session stays up. Step under the
    // TTL so a re-assert always lands between sweeps.
    for(int i = 0; i < 10; ++i)
        net.advance(std::chrono::milliseconds(200));

    REQUIRE(net.r.is_connected(net.id_o)); // the live path never dropped
    REQUIRE(net.downstream_sees_origin() == 1);
}

TEST_CASE("relay refresh: a downstream degrades origins via a relay whose session dies to unreachable, not disappeared", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    relay_net net;
    net.establish();
    REQUIRE(net.downstream_sees_origin() == 1);
    REQUIRE(net.origin_reach_status() == plexus::graph::reachability::reachable);

    // D's only path to O is via R. Tearing down D's session to R degrades O to UNREACHABLE-NOT-DEAD: the
    // dead relay can no longer withdraw O, but O's identity and its via edge are RETAINED (distinguishable
    // from a peer that genuinely left) so the path recovers if R returns — never conflated with disappeared.
    net.d.session_for(inbound_slot(1))->tear_down();
    net.drive();

    REQUIRE(net.downstream_sees_origin() == 1); // the row is kept, not retired
    REQUIRE(net.origin_reach_status() == plexus::graph::reachability::unreachable);
}

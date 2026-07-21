// The relay emit + withdrawal wiring driven end to end over the inproc virtual clock: a relay dials an
// origin and a downstream, lifts the origin from held session state, and asserts it to the downstream
// as a reported candidate. Losing the LIVE PATH to the origin — an origin goodbye, the origin's
// session death, or a TTL age-out of its awareness — emits a withdrawal-flagged report the downstream
// retires on AND drops the origin from the relay's replay set, so a dead origin is never re-refreshed.

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

plexus::io::reconnect_config slow_redial()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(10000), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

// A relay R dialing an origin O and a downstream D on one bus. R carries the real emitter; O and D are
// plain nodes. R re-announces each attached peer to the other, so D learns O as a reported candidate.
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
    plexus::io::endpoint ep_o{ep_for("node-o")};
    plexus::io::endpoint ep_d{ep_for("node-d")};

    explicit relay_net(plexus::io::liveliness_options live = {}, plexus::io::reconnect_config r_redial = forever_cfg())
            : r(tr_r, ex, make_cfg(0xA1), k_long_timeout, r_redial, k_seed, sink, false, plexus::io::global_default_max_message_bytes, live)
            , o(tr_o, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , d(tr_d, ex, make_cfg(0xD4), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        r.listen(ep_for("node-r"));
        o.listen(ep_o);
        d.listen(ep_d);
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
        r.note_peer(id_o, ep_o);
        r.reach(id_o);
        r.note_peer(id_d, ep_d);
        r.reach(id_d);
        drive();
    }

    std::size_t downstream_sees_origin() const
    {
        return d.known().candidates(id_o).size();
    }
};

}

TEST_CASE("relay emitter: an attached origin is asserted to the downstream and held in the replay set", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    relay_net net;
    net.establish();

    REQUIRE(net.r.is_connected(net.id_o));
    REQUIRE(net.r.is_connected(net.id_d));
    REQUIRE(net.r.reports_origin(net.id_o));
    REQUIRE(net.r.reports_origin(net.id_d));
    REQUIRE(net.downstream_sees_origin() == 1);
}

TEST_CASE("relay emitter: the origin's session death withdraws it downstream and shrinks the replay set", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    relay_net net;
    net.establish();
    REQUIRE(net.downstream_sees_origin() == 1);

    net.r.session_for(net.id_o)->tear_down();
    net.drive();

    REQUIRE_FALSE(net.r.reports_origin(net.id_o));
    REQUIRE(net.r.reports_origin(net.id_d));
    REQUIRE(net.downstream_sees_origin() == 0);
}

TEST_CASE("relay emitter: an origin goodbye withdraws it downstream and shrinks the replay set", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    relay_net net;
    net.establish();
    REQUIRE(net.downstream_sees_origin() == 1);

    net.r.forget(net.id_o);
    net.drive();

    REQUIRE_FALSE(net.r.reports_origin(net.id_o));
    REQUIRE(net.downstream_sees_origin() == 0);
}

TEST_CASE("relay emitter: a TTL age-out of the origin's awareness withdraws it downstream", "[graph][peer_report][relay]")
{
    manual_clock::reset();
    plexus::io::liveliness_options live;
    live.awareness_ttl = std::chrono::milliseconds(10);
    relay_net net{live, slow_redial()};
    net.establish();
    REQUIRE(net.downstream_sees_origin() == 1);

    // Drop the origin's accepted end so the relay's dialer session sees a bare drop; with a slow redial
    // the disconnect teardown is deferred, so the awareness sweep at the next tick is the live-path loss.
    net.o.session_for(inbound_slot(1))->tear_down();
    net.advance(std::chrono::milliseconds(100));

    REQUIRE_FALSE(net.r.reports_origin(net.id_o));
    REQUIRE(net.downstream_sees_origin() == 0);
}

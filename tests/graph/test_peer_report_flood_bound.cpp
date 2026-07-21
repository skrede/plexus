// The receiver's flood bound against a hostile authenticated relay: a single reporter may install
// only a capped number of distinct reported origins and a bounded topic set per origin, each excess
// report rejected-and-counted without evicting a held row or perturbing a direct peer. Also pins the
// reporter-is-origin self-report guard. Drives the shared support/graph_change_inproc.h harness.

#include "support/graph_change_inproc.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/peer_report.h"
#include "plexus/wire/topic_declaration.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>
#include <cstdint>

using namespace plexus::testing::graph_change_fixture;
using plexus::node_id;
namespace wire = plexus::wire;

namespace {

wire::peer_report assert_report(const node_id &origin, std::uint16_t seq, std::size_t topics = 0)
{
    wire::peer_report pr;
    pr.origin          = origin;
    pr.origin_universe = plexus::discovery::k_default_universe;
    pr.hop             = 1;
    pr.seq             = seq;
    pr.flags           = wire::k_peer_report_consent_flag | wire::k_peer_report_topics_flag;
    for(std::size_t i = 0; i < topics; ++i)
        pr.topics.push_back(wire::topic_declaration{
                .topic_hash = i + 1, .type_id = 0, .fqn = "t/" + std::to_string(i), .type_name = "", .state = wire::type_state::undeclared});
    return pr;
}

struct bounded_report_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t transport{ex, bus};
    plexus::log::null_logger sink;
    engine eng;
    node_id reporter{make_id(0xB2)};

    static plexus::io::report_options caps()
    {
        plexus::io::report_options r;
        r.max_reported_origins = 4;
        r.max_report_topics    = 3;
        return r;
    }

    bounded_report_node()
            : eng(transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, {}, {}, caps())
    {
        eng.listen(ep_for("node-a"));
    }

    void ingest(const node_id &rep, const wire::peer_report &pr)
    {
        eng.ingest_peer_report(rep, pr);
        ex.drain();
    }

    std::size_t rows(const node_id &id) const
    {
        return eng.known().candidates(id).size();
    }
};

}

TEST_CASE("peer_report flood: a reporter's origin flood is capped, a direct peer is untouched", "[graph][peer_report][flood]")
{
    bounded_report_node n;
    const node_id direct = make_id(0x77);
    n.eng.note_peer(direct, ep_for("peer-d"));
    n.ex.drain();

    for(int i = 0; i < 50; ++i)
        n.ingest(n.reporter, assert_report(make_id(static_cast<std::uint8_t>(0x10 + i)), 1));

    std::size_t held = 0;
    for(int i = 0; i < 50; ++i)
        held += static_cast<std::size_t>(n.rows(make_id(static_cast<std::uint8_t>(0x10 + i))) != 0);

    REQUIRE(held == 4);                              // exactly the per-reporter ceiling
    REQUIRE(n.eng.reported_dropped_count() == 46);   // honest reject-and-count
    REQUIRE(n.rows(direct) == 1);                    // the direct peer never yielded a row
}

TEST_CASE("peer_report flood: a per-origin topic flood drops the whole report", "[graph][peer_report][flood]")
{
    bounded_report_node n;
    const node_id origin = make_id(0x33);

    n.ingest(n.reporter, assert_report(origin, 1, 4)); // 4 > max_report_topics (3): dropped whole
    REQUIRE(n.rows(origin) == 0);
    REQUIRE(n.eng.reported_dropped_count() == 1);

    n.ingest(n.reporter, assert_report(origin, 2, 3)); // at the ceiling: admitted
    REQUIRE(n.rows(origin) == 1);
}

TEST_CASE("peer_report flood: a reporter self-report installs no transitive twin", "[graph][peer_report][flood]")
{
    bounded_report_node n;
    // origin == reporter: a peer self-reporting must not appear twice (a via-itself row beside its
    // own direct row); the receiver drops it at the gate.
    n.ingest(n.reporter, assert_report(n.reporter, 1));
    REQUIRE(n.rows(n.reporter) == 0);
}

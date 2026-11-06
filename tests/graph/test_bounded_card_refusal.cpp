#include "support/graph_change_inproc.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

using namespace plexus::testing::graph_change_fixture;

namespace {

// The engine keeps its peer table private, so the storage mirrors its own tally after every put.
struct tallied_storage : plexus::io::fixed_peer_storage<8>
{
    static inline std::size_t tally = 0;

    bool put(const plexus::node_id &id, const plexus::io::endpoint &ep, std::uint64_t now)
    {
        const bool admitted = fixed_peer_storage::put(id, ep, now);
        tally               = dropped();
        return admitted;
    }
};

using tallied_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, tallied_storage, plexus::graph::fixed_topic_storage<8>>;

}

TEST_CASE("bounded engine: a ninth announced card is dropped and counted while the eight known peers stay reachable", "[graph][bounded]")
{
    manual_clock::reset();
    basic_one_node<tallied_engine> node;

    for(std::uint8_t i = 1; i <= 8; ++i)
        node.eng.note_peer(make_id(i), ep_for("peer"));
    node.drain();
    const std::size_t before = tallied_storage::tally;

    REQUIRE_NOTHROW(node.eng.note_peer(make_id(9), ep_for("ninth")));
    node.drain();

    REQUIRE(tallied_storage::tally == before + 1);
    REQUIRE_FALSE(node.eng.known().contains(make_id(9)));
    for(std::uint8_t i = 1; i <= 8; ++i)
        REQUIRE(node.eng.known().dial_target(make_id(i)) == ep_for("peer"));
}

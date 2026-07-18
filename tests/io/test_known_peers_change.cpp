// The change-detection contract on the peer awareness mutators: note_peer / forget (and the
// underlying put / remove) report whether they actually altered the surface, so the graph
// observer can bump its generation only on a real change and stay silent on an idle re-announce.
// Both storage twins (the std::map default and the fixed-capacity variant) carry the IDENTICAL
// bool return — INV-1 symmetric. Header-only core, Catch2 main only.

#include "plexus/io/known_peers.h"
#include "plexus/io/fixed_peer_storage.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

using plexus::io::basic_known_peers;
using plexus::io::endpoint;
using plexus::io::fixed_peer_storage;
using plexus::io::known_peers;
using plexus::node_id;

namespace {

node_id id_of(std::byte b)
{
    node_id id{};
    id[0] = b;
    return id;
}

endpoint ep_of(const char *addr)
{
    return endpoint{.scheme = "tcp", .address = addr};
}

// The change-signal contract, run against any basic_known_peers<Storage>: a new id is a change,
// a same-(id,endpoint) re-announce is not, an endpoint move is a change, a present forget is a
// change, an absent forget is not.
template<typename Table>
void exercise_change_signal(Table &table)
{
    const node_id a = id_of(std::byte{0x01});

    REQUIRE(table.note_peer(a, ep_of("a:1")) == true);        // new id
    REQUIRE(table.note_peer(a, ep_of("a:1")) == false);       // idempotent re-announce
    REQUIRE(table.note_peer(a, ep_of("a:2")) == true);        // endpoint moved (reachability change)
    REQUIRE(table.note_peer(a, ep_of("a:2")) == false);       // settled again

    REQUIRE(table.forget(a) == true);                         // erased
    REQUIRE(table.forget(a) == false);                        // nothing to erase
}

} // namespace

TEST_CASE("known_peers_change default storage reports a change only on a real add/move/remove", "[io][known_peers]")
{
    known_peers table;
    exercise_change_signal(table);
}

TEST_CASE("known_peers_change fixed variant carries the identical change signal", "[io][known_peers]")
{
    basic_known_peers<fixed_peer_storage<8>> table;
    exercise_change_signal(table);
}

TEST_CASE("known_peers_change the timestamped note_peer forwards the storage change bool", "[io][known_peers]")
{
    const node_id a = id_of(std::byte{0x01});

    known_peers heap;
    REQUIRE(heap.note_peer(a, ep_of("a:1"), 100) == true);
    REQUIRE(heap.note_peer(a, ep_of("a:1"), 200) == false); // refresh only, no surface change

    basic_known_peers<fixed_peer_storage<8>> fixed;
    REQUIRE(fixed.note_peer(a, ep_of("a:1"), 100) == true);
    REQUIRE(fixed.note_peer(a, ep_of("a:1"), 200) == false);
}

// The known_peers storage-policy oracle: proves basic_known_peers<Storage> keeps the
// 4-verb awareness contract (note_peer / lookup / contains / forget) byte-for-byte across
// the std::map default (the PC type) and a dep-free flat fixed-capacity variant, and that
// the fixed variant fails closed on over-capacity (the N+1-th distinct identity aborts via
// fail_closed, never an out-of-bounds write or a silent drop). No socket, no backend —
// header-only core, linked against plexus::core + Catch2's main only.

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

// The full 4-verb contract, run against any basic_known_peers<Storage> instance: insert,
// overwrite, lookup-present, lookup-absent, contains, forget. Shared so the default and the
// fixed variant prove the identical surface.
template<typename Table>
void exercise_four_verbs(Table &table)
{
    const node_id a = id_of(std::byte{0x01});
    const node_id b = id_of(std::byte{0x02});

    REQUIRE_FALSE(table.contains(a));
    REQUIRE_FALSE(table.lookup(a).has_value());

    table.note_peer(a, ep_of("a:1"));
    REQUIRE(table.contains(a));
    REQUIRE(table.lookup(a) == ep_of("a:1"));

    // Overwrite keeps a single entry, replacing the endpoint.
    table.note_peer(a, ep_of("a:2"));
    REQUIRE(table.lookup(a) == ep_of("a:2"));

    table.note_peer(b, ep_of("b:1"));
    REQUIRE(table.contains(b));
    REQUIRE(table.lookup(b) == ep_of("b:1"));

    table.forget(a);
    REQUIRE_FALSE(table.contains(a));
    REQUIRE_FALSE(table.lookup(a).has_value());
    REQUIRE(table.contains(b)); // forgetting one leaves the other
}

} // namespace

TEST_CASE("known_peers_storage default storage reproduces the std::map 4-verb behavior", "[io][known_peers]")
{
    known_peers table; // == basic_known_peers<> == the std::map PC default
    exercise_four_verbs(table);
}

TEST_CASE("known_peers_storage fixed variant satisfies the same 4-verb contract", "[io][known_peers]")
{
    basic_known_peers<fixed_peer_storage<8>> table;
    exercise_four_verbs(table);
}

TEST_CASE("known_peers_storage fixed variant fills to capacity and overwrites in place", "[io][known_peers]")
{
    constexpr std::size_t                      cap = 4;
    basic_known_peers<fixed_peer_storage<cap>> table;

    for(std::size_t i = 0; i < cap; ++i)
        table.note_peer(id_of(std::byte(i + 1)), ep_of("x"));

    // All cap entries present.
    for(std::size_t i = 0; i < cap; ++i)
        REQUIRE(table.contains(id_of(std::byte(i + 1))));

    // Overwriting an existing id at capacity does NOT exceed capacity (no new slot).
    table.note_peer(id_of(std::byte{0x01}), ep_of("y"));
    REQUIRE(table.lookup(id_of(std::byte{0x01})) == ep_of("y"));

    // A free slot reopens after a forget — a fresh distinct id fits again.
    table.forget(id_of(std::byte{0x02}));
    table.note_peer(id_of(std::byte{0x09}), ep_of("z"));
    REQUIRE(table.contains(id_of(std::byte{0x09})));
}

TEST_CASE("known_peers_storage fixed variant fails closed on over-capacity", "[io][known_peers]")
{
    constexpr std::size_t                      cap = 2;
    basic_known_peers<fixed_peer_storage<cap>> table;

    table.note_peer(id_of(std::byte{0x01}), ep_of("a"));
    table.note_peer(id_of(std::byte{0x02}), ep_of("b"));

    // The (cap+1)-th DISTINCT identity has no matching and no free slot: fail closed.
    // Under exceptions (the PC build) fail_closed throws std::runtime_error; it is NEVER a
    // silent drop, an out-of-bounds write, or an unconditional terminate.
    REQUIRE_THROWS_AS(table.note_peer(id_of(std::byte{0x03}), ep_of("c")), std::runtime_error);
}

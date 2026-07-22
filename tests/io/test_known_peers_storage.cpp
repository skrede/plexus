// The known_peers storage-policy oracle: proves basic_known_peers<Storage> keeps the
// 4-verb awareness contract (note_peer / lookup / contains / forget) byte-for-byte across
// the std::map default (the PC type) and a dep-free flat fixed-capacity variant, and that
// the fixed variant fails closed on over-capacity (the N+1-th distinct identity aborts via
// fail_closed, never an out-of-bounds write or a silent drop). No socket, no backend —
// header-only core, linked against plexus::core + Catch2's main only.

#include "plexus/io/known_peers.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/fixed_peer_storage.h"

#include "plexus/graph/participant_record.h"

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <vector>
#include <cstddef>
#include <optional>

using plexus::io::basic_known_peers;
using plexus::io::endpoint;
using plexus::io::fixed_peer_storage;
using plexus::io::known_peers;
using plexus::io::route_candidate;
using plexus::io::route_options;
using plexus::node_id;
using plexus::graph::observation;
using plexus::graph::provenance;
using plexus::graph::reachability;
using plexus::graph::route;

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

// A reachability-hearsay row: a via-only candidate reached through `via`, the shape note_reported
// installs (an empty transport, never a dial target).
route_candidate relayed_via(const node_id &via)
{
    route_candidate c{};
    c.reach  = route{endpoint{}, via};
    c.origin = provenance{observation::reported, via};
    c.hop    = 1;
    return c;
}

// The reachability the (origin, via) transitive row currently carries (reachable when the row is absent).
template<typename Table>
reachability reach_status_via(const Table &table, const node_id &id, const node_id &via)
{
    for(const route_candidate &c : table.candidates(id))
        if(!c.is_direct() && c.reach.via == via)
            return c.origin.reach_status;
    return reachability::reachable;
}

// The mark/recover lifecycle a relay-death drives, run against any basic_known_peers<Storage>: a
// reported row starts reachable, degrades to unreachable IN PLACE (identity retained), re-marks
// idempotently, and its recovery counterpart restores it — with a mark for an unknown (origin, via)
// a no-op.
template<typename Table>
void exercise_unreachable_marking(Table &table)
{
    const node_id origin = id_of(std::byte{0x11});
    const node_id relay  = id_of(std::byte{0x22});

    table.note_reported(origin, relayed_via(relay), 1, 0, route_options{});
    REQUIRE(table.contains(origin));
    REQUIRE(reach_status_via(table, origin, relay) == reachability::reachable);

    // Relay death degrades the row without retiring the identity or its via edge.
    REQUIRE(table.mark_unreachable_via(origin, relay));
    REQUIRE(table.contains(origin));
    REQUIRE(reach_status_via(table, origin, relay) == reachability::unreachable);
    REQUIRE_FALSE(table.mark_unreachable_via(origin, relay)); // idempotent: no further transition

    REQUIRE(table.mark_reachable_via(origin, relay));
    REQUIRE(reach_status_via(table, origin, relay) == reachability::reachable);
    REQUIRE_FALSE(table.mark_reachable_via(origin, relay));

    REQUIRE_FALSE(table.mark_unreachable_via(id_of(std::byte{0x99}), relay)); // unknown origin
    REQUIRE_FALSE(table.mark_unreachable_via(origin, id_of(std::byte{0x98}))); // unknown via
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

// Collects every (node_id, endpoint) for_each hands over into an ordered map, so the visited
// live set can be compared independent of visitation order.
template<typename Table>
std::map<node_id, endpoint> collect_via_for_each(const Table &table)
{
    std::map<node_id, endpoint> seen;
    table.for_each([&](const node_id &id, const endpoint &ep) { seen[id] = ep; });
    return seen;
}

} // namespace

TEST_CASE("known_peers_storage for_each visits every live pair without erasing", "[io][known_peers]")
{
    const node_id a = id_of(std::byte{0x01});
    const node_id b = id_of(std::byte{0x02});

    known_peers table;
    table.note_peer(a, ep_of("a:1"));
    table.note_peer(b, ep_of("b:1"));

    const auto seen = collect_via_for_each(table);
    REQUIRE(seen.size() == 2);
    REQUIRE(seen.at(a) == ep_of("a:1"));
    REQUIRE(seen.at(b) == ep_of("b:1"));

    // Read-only: the live set survives the sweep.
    REQUIRE(table.contains(a));
    REQUIRE(table.contains(b));
}

TEST_CASE("known_peers_storage fixed for_each skips unoccupied slots and matches the std::map set", "[io][known_peers]")
{
    const node_id a = id_of(std::byte{0x01});
    const node_id b = id_of(std::byte{0x02});
    const node_id c = id_of(std::byte{0x03});

    basic_known_peers<fixed_peer_storage<8>> table;
    table.note_peer(a, ep_of("a:1"));
    table.note_peer(b, ep_of("b:1"));
    table.note_peer(c, ep_of("c:1"));
    table.forget(b); // reopens a slot the sweep must skip

    const auto seen = collect_via_for_each(table);
    REQUIRE(seen.size() == 2);
    REQUIRE(seen.at(a) == ep_of("a:1"));
    REQUIRE(seen.at(c) == ep_of("c:1"));
    REQUIRE_FALSE(seen.count(b));

    REQUIRE(table.contains(a));
    REQUIRE(table.contains(c));
}

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
    constexpr std::size_t cap = 4;
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
    constexpr std::size_t cap = 2;
    basic_known_peers<fixed_peer_storage<cap>> table;

    table.note_peer(id_of(std::byte{0x01}), ep_of("a"));
    table.note_peer(id_of(std::byte{0x02}), ep_of("b"));

    // The (cap+1)-th DISTINCT identity has no matching and no free slot: fail closed.
    // Under exceptions (the PC build) fail_closed throws std::runtime_error; it is NEVER a
    // silent drop, an out-of-bounds write, or an unconditional terminate.
    REQUIRE_THROWS_AS(table.note_peer(id_of(std::byte{0x03}), ep_of("c")), std::runtime_error);
}

TEST_CASE("known_peers_storage default twin marks a reported row unreachable in place and recovers it", "[io][known_peers]")
{
    known_peers table; // the std::map PC default
    exercise_unreachable_marking(table);
}

TEST_CASE("known_peers_storage fixed twin marks a reported row unreachable in place and recovers it", "[io][known_peers]")
{
    basic_known_peers<fixed_peer_storage<8>> table;
    exercise_unreachable_marking(table);
}

// INV-1: a direct-only identity's for_each_candidate enumeration is byte-identical whether or not a
// SEPARATE identity's transitive row is marked unreachable — the flag never leaks into a direct row.
namespace {

struct enum_row
{
    node_id id;
    std::optional<node_id> via;
    observation how;
    reachability reach_status;
    bool operator==(const enum_row &) const = default;
};

template<typename Table>
std::vector<enum_row> direct_only_rows(const Table &table, const node_id &want)
{
    std::vector<enum_row> rows;
    table.for_each_candidate([&](const node_id &id, const route &reach, const provenance &origin) {
        if(id == want)
            rows.push_back(enum_row{id, reach.via, origin.how, origin.reach_status});
    });
    return rows;
}

} // namespace

TEST_CASE("known_peers_storage direct-only enumeration is byte-identical across a transitive unreachable mark", "[io][known_peers][inv1]")
{
    const node_id direct = id_of(std::byte{0x0D});
    const node_id origin = id_of(std::byte{0x11});
    const node_id relay  = id_of(std::byte{0x22});

    known_peers table;
    table.note_peer(direct, ep_of("d:1"));
    table.note_reported(origin, relayed_via(relay), 1, 0, route_options{});

    const auto before = direct_only_rows(table, direct);
    REQUIRE(before.size() == 1);
    REQUIRE(before[0].via == std::nullopt);
    REQUIRE(before[0].how == observation::directly_observed);
    REQUIRE(before[0].reach_status == reachability::reachable);

    REQUIRE(table.mark_unreachable_via(origin, relay));

    // The direct-only identity's enumeration did not move: the mark stayed on the transitive row.
    REQUIRE(direct_only_rows(table, direct) == before);
}

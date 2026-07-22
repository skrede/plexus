// The fixed twin's direct-first candidate admission: each identity holds a bounded, inline candidate
// list where a direct row is never displaced by a transitive flood, transitive overflow is
// rejected-and-counted (never an abort, never an eviction of a direct row), and lookup surfaces the
// direct endpoint only. Proven under BOTH protection schemes with no node, no I/O — header-only core.

#include "plexus/io/endpoint.h"
#include "plexus/io/route_options.h"
#include "plexus/io/route_candidate.h"
#include "plexus/io/fixed_peer_storage.h"

#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/wire/udp_dedup_window.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstdint>
#include <stdexcept>

namespace
{

using plexus::node_id;
using plexus::graph::observation;
using plexus::graph::provenance;
using plexus::graph::reachability;
using plexus::graph::route;
using plexus::io::direct_protection;
using plexus::io::endpoint;
using plexus::io::fixed_peer_storage;
using plexus::io::route_candidate;
using plexus::io::route_options;
using plexus::io::detail::report_admit;

node_id id_with(std::uint8_t tag)
{
    node_id id{};
    id[0] = std::byte{tag};
    return id;
}

endpoint ep_with(std::uint8_t tag)
{
    return endpoint{"udp", "a:" + std::to_string(tag)};
}

route_candidate direct_with(std::uint8_t tag)
{
    return route_candidate{route{ep_with(tag), std::nullopt}, provenance{observation::directly_observed, std::nullopt}, 0, plexus::wire::udp_dedup_window{}, 0};
}

route_candidate relayed_via(std::uint8_t via)
{
    return route_candidate{route{ep_with(static_cast<std::uint8_t>(0x80 + via)), id_with(via)}, provenance{observation::reported, id_with(via)}, 1, plexus::wire::udp_dedup_window{}, 0};
}

std::size_t direct_rows(std::span<const route_candidate> rows)
{
    std::size_t n = 0;
    for(const route_candidate &c : rows)
        n += static_cast<std::size_t>(c.is_direct());
    return n;
}

}

TEST_CASE("fixed twin reserved_slots keeps the direct row through a transitive flood", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{direct_protection::reserved_slots, 1};
    const node_id peer = id_with(1);

    REQUIRE(store.admit(peer, direct_with(1), 10, opts));
    for(std::uint8_t k = 0; k < 10; ++k)
        store.admit(peer, relayed_via(static_cast<std::uint8_t>(20 + k)), 20, opts);

    REQUIRE(store.get(peer) == ep_with(1)); // the direct survives and is still surfaced
    REQUIRE(store.candidates(peer).size() == 4);
    REQUIRE(direct_rows(store.candidates(peer)) == 1);
    REQUIRE(store.dropped() == 7); // cap 4 - reserved 1 = 3 transitive admitted, 7 rejected
}

TEST_CASE("fixed twin reserved_slots holds a slot open so a late direct is admitted without eviction", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{direct_protection::reserved_slots, 1};
    const node_id peer = id_with(1);

    for(std::uint8_t k = 0; k < 10; ++k)
        store.admit(peer, relayed_via(static_cast<std::uint8_t>(20 + k)), 20, opts);
    REQUIRE(store.candidates(peer).size() == 3); // reserved slot held open for a direct
    REQUIRE(store.dropped() == 7);

    REQUIRE(store.admit(peer, direct_with(1), 30, opts));
    REQUIRE(store.candidates(peer).size() == 4);
    REQUIRE(direct_rows(store.candidates(peer)) == 1);
    REQUIRE(store.get(peer) == ep_with(1));
    REQUIRE(store.dropped() == 7); // no transitive evicted to seat the direct
}

TEST_CASE("fixed twin priority_evict yields a transitive row to a late-arriving direct", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{direct_protection::priority_evict, 0};
    const node_id peer = id_with(1);

    for(std::uint8_t k = 0; k < 10; ++k)
        store.admit(peer, relayed_via(static_cast<std::uint8_t>(20 + k)), 20, opts);
    REQUIRE(store.candidates(peer).size() == 4); // transitive fills every free row
    REQUIRE(direct_rows(store.candidates(peer)) == 0);
    REQUIRE_FALSE(store.get(peer).has_value()); // no via candidate surfaced to a dial
    REQUIRE(store.dropped() == 6);

    REQUIRE(store.admit(peer, direct_with(1), 30, opts));
    REQUIRE(store.candidates(peer).size() == 4);
    REQUIRE(direct_rows(store.candidates(peer)) == 1); // a transitive yielded its row
    REQUIRE(store.get(peer) == ep_with(1));
    REQUIRE(store.dropped() == 6); // an eviction for a direct is not a reject-and-count
}

TEST_CASE("fixed twin re-report is idempotent and a fresh direct endpoint overwrites in place", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{};
    const node_id peer = id_with(1);

    REQUIRE(store.admit(peer, direct_with(1), 10, opts));
    REQUIRE_FALSE(store.admit(peer, direct_with(1), 11, opts)); // same direct: no change
    REQUIRE(store.admit(peer, direct_with(2), 12, opts));       // fresh endpoint: changed
    REQUIRE(store.get(peer) == ep_with(2));
    REQUIRE(direct_rows(store.candidates(peer)) == 1);

    REQUIRE(store.admit(peer, relayed_via(20), 13, opts));
    REQUIRE_FALSE(store.admit(peer, relayed_via(20), 14, opts)); // same (via, transport): no change
}

TEST_CASE("fixed twin lookup surfaces the direct endpoint only, never a via candidate", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{};
    const node_id peer = id_with(1);

    store.admit(peer, relayed_via(20), 10, opts);
    REQUIRE(store.has(peer));                    // the identity exists
    REQUIRE_FALSE(store.get(peer).has_value());  // but no direct endpoint is surfaced

    store.admit(peer, direct_with(1), 11, opts);
    REQUIRE(store.get(peer) == ep_with(1));
}

TEST_CASE("fixed twin reported window anchors a high first seq and reset re-arms after a reporter restart", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{};
    const node_id peer = id_with(1);
    const node_id via  = id_with(20);

    // First sighting at a seq already past the serial half-space anchors the row's window (noted_new),
    // so a following higher seq REFRESHES instead of being classified too_old against a default 0.
    REQUIRE(store.note_reported(peer, relayed_via(20), 40000, 10, opts) == report_admit::noted_new);
    REQUIRE(store.note_reported(peer, relayed_via(20), 40001, 11, opts) == report_admit::refreshed);
    // A stale seq more than depth behind the anchored high-water is a no-op (neither refreshes nor churns).
    REQUIRE(store.note_reported(peer, relayed_via(20), 39000, 12, opts) == report_admit::duplicate);

    // The reporter restarts and its per-origin counter drops back to a low seq; reset re-arms the
    // first-sighting anchor so the post-restart replay re-admits instead of deduping behind the mark.
    REQUIRE(store.reset_reported_windows(via) == 1);
    REQUIRE(store.note_reported(peer, relayed_via(20), 1, 13, opts) == report_admit::refreshed);
}

reachability status_via(const fixed_peer_storage<4, 4> &store, const node_id &id, const node_id &via)
{
    for(const route_candidate &c : store.candidates(id))
        if(!c.is_direct() && c.reach.via == via)
            return c.origin.reach_status;
    return reachability::reachable;
}

TEST_CASE("fixed twin marks a via row unreachable in place, keeping the row and its direct twin reachable", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<4, 4> store;
    const route_options opts{};
    const node_id peer = id_with(1);
    const node_id via  = id_with(20);

    REQUIRE(store.admit(peer, direct_with(1), 10, opts)); // a direct row alongside the relayed one
    REQUIRE(store.note_reported(peer, relayed_via(20), 1, 10, opts) == report_admit::noted_new);
    REQUIRE(status_via(store, peer, via) == reachability::reachable);

    // Relay death degrades only the via row; the identity, the row, and the direct twin survive.
    REQUIRE(store.mark_unreachable_via(peer, via));
    REQUIRE(store.candidates(peer).size() == 2);
    REQUIRE(direct_rows(store.candidates(peer)) == 1);
    REQUIRE(store.get(peer) == ep_with(1)); // the direct endpoint is still surfaced, always reachable
    REQUIRE(status_via(store, peer, via) == reachability::unreachable);
    REQUIRE_FALSE(store.mark_unreachable_via(peer, via)); // idempotent

    // The recovery counterpart restores it; an unknown (origin, via) marks nothing.
    REQUIRE(store.mark_reachable_via(peer, via));
    REQUIRE(status_via(store, peer, via) == reachability::reachable);
    REQUIRE_FALSE(store.mark_unreachable_via(id_with(2), via));
}

TEST_CASE("fixed twin fails closed on a direct peer past identity capacity but never on a transitive flood", "[graph][route][fixed_peer_storage]")
{
    fixed_peer_storage<2, 4> store;
    const route_options opts{};

    store.admit(id_with(1), direct_with(1), 10, opts);
    store.admit(id_with(2), direct_with(2), 10, opts);

    // A transitive report about a new identity when the table is full is rejected-and-counted, never
    // an abort — the DoS guard for T-94-02.
    REQUIRE_FALSE(store.admit(id_with(3), relayed_via(3), 20, opts));
    REQUIRE(store.dropped() == 1);
    REQUIRE_FALSE(store.has(id_with(3)));

    // A direct peer past the identity capacity is the DEFINED refusal (fail_closed throws under the
    // exception-enabled PC build).
    REQUIRE_THROWS_AS(store.admit(id_with(4), direct_with(4), 20, opts), std::runtime_error);
}

// The pure forward-admission chain oracle: every outcome (admit / loop / hop / duplicate / too_old) is
// distinct, a rejected frame mutates no dedup window, and the (origin, arrival-relay) key discriminates
// two relays minting independent seq streams for one origin. No I/O, no session — the gate is a pure
// free function over a forwarded_frame and a dedup table.

#include "plexus/io/detail/forward_gate.h"

#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

namespace wire = plexus::wire;
using plexus::node_id;
using plexus::io::detail::forward_gate;
using plexus::io::detail::forward_admission;
using plexus::io::detail::forward_dedup_table;

namespace {

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

constexpr std::uint8_t k_hop_budget = 8;
constexpr std::size_t k_depth       = 32;

const node_id k_local = id_of(0x01);
const node_id k_relay = id_of(0x20);
const node_id k_origin = id_of(0xB0);

wire::forwarded_frame frame_from(const node_id &origin, std::uint8_t hop, std::uint16_t seq)
{
    wire::forwarded_frame ff;
    ff.origin      = origin;
    ff.destination = k_local;
    ff.hop         = hop;
    ff.seq         = seq;
    ff.flags       = 0;
    return ff;
}

forward_admission gate(forward_dedup_table &t, const wire::forwarded_frame &ff, const node_id &relay = k_relay)
{
    return forward_gate(ff, k_local, relay, k_hop_budget, k_depth, t);
}

}

TEST_CASE("forward_gate: a fresh in-budget non-loop frame admits", "[forwarder][forward_gate]")
{
    forward_dedup_table table;
    REQUIRE(gate(table, frame_from(k_origin, 1, 42)) == forward_admission::admit);
    REQUIRE(table.tracked() == 1);
}

TEST_CASE("forward_gate: a self/loop frame rejects and mutates no window", "[forwarder][forward_gate]")
{
    forward_dedup_table table;

    // origin is the local identity: a frame that looped back to us.
    REQUIRE(gate(table, frame_from(k_local, 1, 7)) == forward_admission::drop_loop);
    // origin equals the arriving relay: a relay re-wrapping its own direct traffic.
    REQUIRE(gate(table, frame_from(k_relay, 1, 7)) == forward_admission::drop_loop);
    // The arriving relay is us (defensive): still a loop.
    REQUIRE(gate(table, frame_from(k_origin, 1, 7), k_local) == forward_admission::drop_loop);

    REQUIRE(table.tracked() == 0); // no window created for any rejected frame
}

TEST_CASE("forward_gate: a frame over the hop budget rejects and mutates no window", "[forwarder][forward_gate]")
{
    forward_dedup_table table;

    REQUIRE(gate(table, frame_from(k_origin, k_hop_budget, 5)) == forward_admission::admit); // at budget: admitted
    REQUIRE(gate(table, frame_from(k_origin, k_hop_budget + 1, 6)) == forward_admission::drop_hop);

    // Exactly one window exists (from the admitted frame); the over-budget frame created none.
    REQUIRE(table.tracked() == 1);
}

TEST_CASE("forward_gate: a duplicate seq rejects distinctly from too_old", "[forwarder][forward_gate]")
{
    forward_dedup_table table;

    REQUIRE(gate(table, frame_from(k_origin, 1, 1000)) == forward_admission::admit);
    // The same seq again on the same (origin, relay): a replay.
    REQUIRE(gate(table, frame_from(k_origin, 1, 1000)) == forward_admission::drop_duplicate);
    // A seq far below the window (older than depth): too_old, a distinct outcome.
    REQUIRE(gate(table, frame_from(k_origin, 1, 100)) == forward_admission::drop_too_old);
}

TEST_CASE("forward_gate: the (origin, arrival-relay) key discriminates independent relay seq streams", "[forwarder][forward_gate]")
{
    forward_dedup_table table;
    const node_id relay_a = id_of(0x20);
    const node_id relay_b = id_of(0x40);

    // Relay A admits seq 42 for the origin.
    REQUIRE(gate(table, frame_from(k_origin, 1, 42), relay_a) == forward_admission::admit);
    // Relay B minting the SAME seq 42 for the SAME origin does NOT collide — a separate window admits it.
    REQUIRE(gate(table, frame_from(k_origin, 1, 42), relay_b) == forward_admission::admit);
    // A replay on relay A is still a duplicate; relay B's stream is unaffected.
    REQUIRE(gate(table, frame_from(k_origin, 1, 42), relay_a) == forward_admission::drop_duplicate);
    REQUIRE(gate(table, frame_from(k_origin, 1, 43), relay_b) == forward_admission::admit);

    REQUIRE(table.tracked() == 2); // one window per (origin, relay) pair
}

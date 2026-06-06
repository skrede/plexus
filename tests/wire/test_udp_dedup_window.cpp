// The UDP dedup window oracle: the RFC-1982 serial-number anti-replay window over
// a uint16 seq. Proves fresh / duplicate / too_old classification, wrap-safety
// across the 65535 -> 0 rollover (the whole point of the serial-number compare),
// the depth_max = 32 invariant, and that reset() re-freshes seq=0 (the
// handshake/session-id transition that the proven core pairs with a local seq
// reset). Pure header-only — no backend.

#include "plexus/wire/udp_dedup_window.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using window = plexus::wire::udp_dedup_window;
using outcome = plexus::wire::udp_dedup_window::outcome;

TEST_CASE("dedup window: a never-seen seq is fresh, a re-presented seq is duplicate", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(0) == outcome::fresh);
    REQUIRE(w.admit(0) == outcome::duplicate);
    REQUIRE(w.admit(1) == outcome::fresh);
    REQUIRE(w.admit(1) == outcome::duplicate);
}

TEST_CASE("dedup window: a forward run is all fresh and in-window reorder is admitted once", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(10) == outcome::fresh);
    REQUIRE(w.admit(11) == outcome::fresh);
    REQUIRE(w.admit(12) == outcome::fresh);
    // An in-window out-of-order arrival is fresh the first time, duplicate the second.
    REQUIRE(w.admit(11) == outcome::duplicate);
    REQUIRE(w.admit(9) == outcome::fresh);
    REQUIRE(w.admit(9) == outcome::duplicate);
}

TEST_CASE("dedup window: a seq below the window floor is too_old", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(100) == outcome::fresh);
    // depth_max = 32: a seq more than 32 below the high-water mark is past the floor.
    REQUIRE(w.admit(100 - 32) == outcome::too_old);
    REQUIRE(w.admit(1) == outcome::too_old);
    // Just inside the floor is still admissible.
    REQUIRE(w.admit(100 - 31) == outcome::fresh);
}

TEST_CASE("dedup window: depth_max is 32 and the seq-width invariant holds", "[udp][dedup]")
{
    REQUIRE(window::depth_max == 32u);
    REQUIRE(window::half_space == 32768u);
    static_assert((1u << 16) >= 4u * window::depth_max, "dedup invariant");
    window w;
    REQUIRE(w.depth() == 32u);
}

TEST_CASE("dedup window: wrap-safe across 65535 -> 0, with a post-wrap replay still duplicate", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(65534) == outcome::fresh);
    REQUIRE(w.admit(65535) == outcome::fresh);
    REQUIRE(w.admit(0) == outcome::fresh);   // 65535 -> 0 is a forward step of 1, not a blackout
    REQUIRE(w.admit(1) == outcome::fresh);
    // A replay of a pre-wrap seq that is still inside the window is a duplicate.
    REQUIRE(w.admit(65535) == outcome::duplicate);
    REQUIRE(w.admit(0) == outcome::duplicate);
}

TEST_CASE("dedup window: a large forward jump clears the bitmap and stays fresh", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(0) == outcome::fresh);
    // A jump beyond the 64-bit bitmap span clears it; the new high-water is fresh.
    REQUIRE(w.admit(1000) == outcome::fresh);
    REQUIRE(w.admit(1000) == outcome::duplicate);
    // The old seq=0 is now far below the floor.
    REQUIRE(w.admit(0) == outcome::too_old);
}

TEST_CASE("dedup window: reset re-freshes seq=0 (the handshake/session-id transition)", "[udp][dedup]")
{
    window w;
    REQUIRE(w.admit(0) == outcome::fresh);
    REQUIRE(w.admit(1) == outcome::fresh);
    REQUIRE(w.admit(2) == outcome::fresh);
    REQUIRE(w.admit(0) == outcome::duplicate);   // still in-window before the reset

    w.reset();

    // After the reset the window is empty: the first post-handshake seq=0 is fresh again.
    REQUIRE(w.admit(0) == outcome::fresh);
    REQUIRE(w.admit(1) == outcome::fresh);
}

#include "plexus/io/shm/shm_selection.h"

#include <catch2/catch_test_macros.hpp>

// The shared-memory selection decision (D-01/D-02/D-03): a same-host pair with a
// qualifying hint resolves to the shared-memory medium; a same-host pair with no
// hint falls back to the stream; a cross-host pair is never shared-memory
// regardless of hint. Plus the per-forwarder acquired-set bookkeeping (no
// singleton; the (node_name, fqn) refcount mirroring subscriber_registry).

using namespace plexus::io::shm;

namespace {
constexpr host_fingerprint k_host_a{0xa11};
constexpr host_fingerprint k_host_b{0xb22};
constexpr host_fingerprint k_null{0};
}

TEST_CASE("selection: a same-host pair with a hint is shared-memory", "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_a, k_host_a, dispatch_hint::frequent)
            == same_host_medium::shm);
    REQUIRE(select_same_host_medium(k_host_a, k_host_a, dispatch_hint::large | dispatch_hint::priority)
            == same_host_medium::shm);
}

TEST_CASE("selection: a same-host pair with no hint falls back to the stream", "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_a, k_host_a, dispatch_hint::none)
            == same_host_medium::stream);
}

TEST_CASE("selection: a cross-host pair is never shared-memory regardless of hint", "[shm][selection]")
{
    REQUIRE(select_same_host_medium(k_host_b, k_host_a, dispatch_hint::frequent)
            == same_host_medium::stream);
    REQUIRE(select_same_host_medium(k_host_b, k_host_a, dispatch_hint::none)
            == same_host_medium::stream);

    // A null peer (advertised nothing / forged zero) is never shared-memory.
    REQUIRE(select_same_host_medium(k_null, k_host_a, dispatch_hint::frequent)
            == same_host_medium::stream);
}

TEST_CASE("selection: the acquired-ring set refcounts the (node_name, fqn) pair", "[shm][selection]")
{
    acquired_ring_set set;

    REQUIRE_FALSE(set.holds("node-b", "topic/x"));

    // 0->1 acquire gate.
    REQUIRE(set.acquire("node-b", "topic/x") == 1);
    REQUIRE(set.holds("node-b", "topic/x"));

    // A second demand on the same pair bumps the count, no second acquire gate.
    REQUIRE(set.acquire("node-b", "topic/x") == 2);

    // A distinct pair is independent.
    REQUIRE(set.acquire("node-b", "topic/y") == 1);
    REQUIRE(set.acquire("node-c", "topic/x") == 1);

    // Release walks back down; the 1->0 result is the release gate.
    REQUIRE(set.release("node-b", "topic/x") == 1);
    REQUIRE(set.release("node-b", "topic/x") == 0);
    REQUIRE_FALSE(set.holds("node-b", "topic/x"));

    // The other pairs are untouched.
    REQUIRE(set.holds("node-b", "topic/y"));
    REQUIRE(set.holds("node-c", "topic/x"));
}

TEST_CASE("selection: releasing an unknown pair reports no transition", "[shm][selection]")
{
    acquired_ring_set set;
    REQUIRE(set.release("ghost", "nope") == acquired_ring_set::k_no_entry);
}

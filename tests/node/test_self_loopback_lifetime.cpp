// The self-route lifetime contract, proven under sanitizers — NO per-callback liveness guard.
//
// (1) retire-then-posted: a publish posts a self-delivery closure; the subscriber is retired BEFORE
//     the executor drains; the drain re-enters dispatch, which re-scans m_subscriptions, finds the
//     retired subscription absent, and is a structural no-op (the callback fires zero times, no UAF).
// (2) drain-before-destroy: the canonical lifecycle — drain the borrowed executor, THEN destroy the
//     node; no posted closure outlives the node. asan/ubsan is the load-bearing gate.

#include "test_self_loopback_common.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <cstddef>

using plexus_test::fixture;
using plexus_test::make_id;
using plexus_test::loopback_policy;

namespace plexus_test {
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::discovery::static_discovery;
}

TEST_CASE("self_loopback_lifetime: retire-then-posted is a safe no-op", "[node][loopback][lifetime]")
{
    fixture f;

    int fired = 0;
    {
        plexus::publisher<> p{f.node(), "topic"};
        {
            plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte>) { ++fired; }};
            f.drive();

            // Publish posts a self-delivery closure; the subscriber retires (its dtor at this inner
            // scope exit) BEFORE the executor drains.
            const std::array<std::byte, 2> payload{std::byte{0x01}, std::byte{0x02}};
            p.publish(payload);
        }
        // The subscriber is gone; the posted closure has NOT run yet. Draining re-enters dispatch,
        // re-scans m_subscriptions (the retired sub is absent), and is a no-op — no dead callback.
        f.drive();
    }
    REQUIRE(fired == 0);
}

TEST_CASE("self_loopback_lifetime: drain-before-destroy holds with no per-callback guard", "[node][loopback][lifetime]")
{
    plexus_test::inproc_bus<> bus;
    plexus_test::inproc_executor<> ex{bus};
    plexus_test::static_discovery disc{{}};

    int fired = 0;
    {
        // The host (and thus the node) lives over the borrowed executor, which outlives it. The
        // endpoints live in an INNER scope so their retire edges (posted by the handle dtors) are
        // drained while the node is still alive — the drain-before-destroy contract.
        plexus::loopback_host<loopback_policy> host{ex, disc, make_id(0x71)};
        {
            plexus::subscriber<> s{host.node(), "topic", [&](std::span<const std::byte>) { ++fired; }};
            plexus::publisher<> p{host.node(), "topic"};
            ex.drain();

            const std::array<std::byte, 3> payload{std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}};
            p.publish(payload);
            ex.drain();
            REQUIRE(fired == 1);
        }
        // The endpoint-retire edges posted at the inner-scope exit are drained here with the node
        // still alive — no posted endpoint closure outlives the engine.
        ex.drain();
    }
    // The node is gone. Its dtor posted only the participant-teardown edge, which the engine emits
    // from a SNAPSHOT (not a live engine reference), so this final pump touches no freed state.
    ex.drain();
    REQUIRE(fired == 1);
}

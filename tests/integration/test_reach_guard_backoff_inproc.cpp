// A completed session resets the redial backoff, so the surrender bounds measure the outage that follows it.

#include "test_reach_guard_common.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>

using namespace reach_guard_fixture;

TEST_CASE("reach guard: a completed session resets the backoff attempt count", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net;
    net.transport_b.close();
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.advance_until([&] { return net.a.attempt_count(net.id_b) >= 3; });
    REQUIRE(net.a.attempt_count(net.id_b) >= 3);

    net.b.listen(net.ep_b);
    net.advance_until([&] { return net.a.is_connected(net.id_b); });
    REQUIRE(net.a.is_connected(net.id_b));
    REQUIRE(net.a.attempt_count(net.id_b) == 0);
}

TEST_CASE("reach guard: the surrender bound measures the outage, not the session's life", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net(bounded_cfg(std::nullopt, std::chrono::seconds(2)));
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto epoch = net.a.session_for(net.id_b)->session_id();
    for(std::int32_t i = 0; i < 100; ++i)
        net.advance(k_tick);
    REQUIRE(net.a.is_connected(net.id_b));

    auto *accepted = net.b.session_for(net.id_a);
    REQUIRE(accepted != nullptr);
    accepted->tear_down();
    net.ex.drain();
    REQUIRE_FALSE(net.a.is_dead(net.id_b));
    net.advance(k_ceiling);
    REQUIRE(net.a.is_connected(net.id_b));
    REQUIRE(net.a.session_for(net.id_b)->session_id() != epoch);
    REQUIRE_FALSE(net.a.is_dead(net.id_b));
}

TEST_CASE("reach guard: a peer that died and later connects is no longer reported dead", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net(bounded_cfg(std::uint32_t{3}, std::nullopt));
    net.transport_b.close();
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.advance_until([&] { return net.a.is_dead(net.id_b); });
    REQUIRE(net.a.is_dead(net.id_b));

    net.b.listen(net.ep_b);
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.is_connected(net.id_b));
    REQUIRE_FALSE(net.a.is_dead(net.id_b));
}

TEST_CASE("reach guard: a session a reach completes during a backoff wait outlives that wait", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net;
    net.transport_b.close();
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.attempt_count(net.id_b) == 1);

    net.b.listen(net.ep_b);
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto epoch = net.a.session_for(net.id_b)->session_id();
    for(std::int32_t i = 0; i < 30; ++i)
        net.advance(k_tick);
    REQUIRE(net.a.is_connected(net.id_b));
    REQUIRE(net.a.session_for(net.id_b)->session_id() == epoch);
    REQUIRE(net.b.registry().slot_count() == 1);
}

// A reach or a dial never disturbs a session that is not torn down.

#include "test_reach_guard_common.h"

#include "plexus/io/endpoint_id.h"

#include "plexus/io/detail/peer_session_slot.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>

using namespace reach_guard_fixture;

namespace {

void require_handshake_intact(guarded_pair &net, const plexus::node_id &key, engine::session_type *s, std::size_t accepted, std::uint32_t attempts)
{
    REQUIRE(net.a.session_for(key) == s);
    REQUIRE_FALSE(s->pending().closing());
    REQUIRE(net.b.registry().slot_count() == accepted);
    REQUIRE(net.a.attempt_count(key) == attempts);
    net.ex.drain();
    REQUIRE(net.a.is_connected(key));
    REQUIRE(net.b.registry().slot_count() == accepted);
    REQUIRE(net.complete_sessions_for_b() == 1);
}

}

TEST_CASE("reach guard: a card arriving while a dial's handshake is in flight leaves the handshake alone", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net(forever_cfg(), true);
    net.a.note_peer(net.id_b, net.ep_b);
    auto *s = net.step_into_handshake(net.id_b);
    REQUIRE(s != nullptr);
    REQUIRE_FALSE(s->is_complete());
    const auto accepted = net.b.registry().slot_count();
    const auto attempts = net.a.attempt_count(net.id_b);

    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    require_handshake_intact(net, net.id_b, s, accepted, attempts);
}

TEST_CASE("reach guard: a repeated dial of an endpoint mid-handshake leaves the handshake alone", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net;
    const auto key = plexus::io::endpoint_id(net.ep_b);
    net.a.dial(net.ep_b);
    auto *s = net.step_into_handshake(key);
    REQUIRE(s != nullptr);
    REQUIRE_FALSE(s->is_complete());
    const auto accepted = net.b.registry().slot_count();
    const auto attempts = net.a.attempt_count(key);

    net.a.dial(net.ep_b);
    require_handshake_intact(net, key, s, accepted, attempts);
}

TEST_CASE("reach guard: a slot that surrendered on a handshake's drop dials again on a fresh card", "[integration][reach_guard]")
{
    manual_clock::reset();
    guarded_pair net(bounded_cfg(std::uint32_t{1}, std::nullopt));
    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    auto *s = net.step_into_handshake(net.id_b);
    REQUIRE(s != nullptr);
    REQUIRE_FALSE(s->is_complete());
    auto *accepted = net.b.session_for(plexus::io::detail::inbound_slot_id(1));
    REQUIRE(accepted != nullptr);
    accepted->tear_down();
    net.ex.drain();
    REQUIRE(net.a.is_dead(net.id_b));
    auto *left = net.a.session_for(net.id_b);
    REQUIRE((left == nullptr || left->pending().closing()));

    net.a.note_peer(net.id_b, net.ep_b);
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.is_connected(net.id_b));
    REQUIRE_FALSE(net.a.is_dead(net.id_b));
}

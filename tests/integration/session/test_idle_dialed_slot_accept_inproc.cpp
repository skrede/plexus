// A dialed slot with nothing pending yields its identity's key to an accepted session that proves that identity.

#include "test_idle_dialed_slot_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/peer_kind.h"

#include "plexus/io/detail/peer_session_complete.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>

using namespace idle_slot_fixture;

namespace {

bool holds_kind(idle_pair &net, plexus::io::peer_kind kind)
{
    auto *s = net.a.session_for(net.id_b);
    return s != nullptr && s->is_complete() && plexus::io::detail::session_kind(*s) == kind;
}

void check_accepted_holds_key(idle_pair &net)
{
    CHECK(net.a.is_connected(net.id_b));
    CHECK(holds_kind(net, plexus::io::peer_kind::accepted));
    CHECK(net.a.registry().slot_count() == 1);
}

}

TEST_CASE("idle dialed slot: an accept from a peer whose dialed slot was torn down locally takes the identity's key", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net;
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));

    net.a.session_for(net.id_b)->tear_down();
    net.ex.drain();
    net.reach_from_b();
    check_accepted_holds_key(net);
}

TEST_CASE("idle dialed slot: an accept from a forgotten peer takes the identity's key", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net;
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));

    net.a.forget(net.id_b);
    net.drop_from_b();
    net.reach_from_b();
    check_accepted_holds_key(net);
}

TEST_CASE("idle dialed slot: an accept from a peer whose dialed slot surrendered takes the identity's key and clears dead", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net(bounded_cfg(std::uint32_t{1}, std::nullopt));
    net.connect();
    net.drop_from_b();
    REQUIRE(net.a.is_dead(net.id_b));

    net.reach_from_b();
    check_accepted_holds_key(net);
    CHECK_FALSE(net.a.is_dead(net.id_b));

    net.b.session_for(net.id_a)->tear_down();
    net.ex.drain();
    net.connect();
    CHECK(holds_kind(net, plexus::io::peer_kind::dialed));
    CHECK(net.a.attempt_count(net.id_b) == 0);
    CHECK_FALSE(net.a.is_dead(net.id_b));
}

TEST_CASE("idle dialed slot: a dialed slot with a backoff armed keeps the identity's key", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net;
    net.a.note_peer(net.id_b, plexus::io::endpoint{"inproc", "nobody"});
    net.a.reach(net.id_b);
    net.ex.drain();
    REQUIRE(net.a.attempt_count(net.id_b) == 1);

    net.reach_from_b();
    CHECK(net.a.attempt_count(net.id_b) >= 1);
    CHECK(net.complete_sessions_for_b() == 1);
    CHECK_FALSE(net.a.is_connected(net.id_b));
}

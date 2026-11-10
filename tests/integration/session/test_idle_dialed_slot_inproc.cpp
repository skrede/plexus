// A dialed slot that will not redial ends a session that drops, so a dead or forgotten slot never reports it complete.

#include "test_idle_dialed_slot_common.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstdint>
#include <optional>

using namespace idle_slot_fixture;

TEST_CASE("idle dialed slot: a slot that surrenders on the drop of a complete session tears that session down", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net(bounded_cfg(std::uint32_t{1}, std::nullopt));
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));

    net.drop_from_b();
    CHECK(net.a.is_dead(net.id_b));
    CHECK_FALSE(net.a.is_connected(net.id_b));
    CHECK(net.log.edges() == std::vector{edge::connected, edge::disconnected, edge::dead});
    auto *s = net.a.session_for(net.id_b);
    CHECK((s == nullptr || s->pending().closing()));
}

TEST_CASE("idle dialed slot: a forgotten slot whose session drops tears that session down", "[integration][idle_slot]")
{
    manual_clock::reset();
    idle_pair net;
    net.connect();
    REQUIRE(net.a.is_connected(net.id_b));
    const auto attempts = net.a.attempt_count(net.id_b);

    net.a.forget(net.id_b);
    net.drop_from_b();
    net.advance(k_ceiling);
    CHECK_FALSE(net.a.is_connected(net.id_b));
    CHECK(net.log.edges() == std::vector{edge::connected, edge::disconnected});
    CHECK(net.a.attempt_count(net.id_b) == attempts);
}

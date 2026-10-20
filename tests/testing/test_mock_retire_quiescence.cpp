#include "plexus/testing/harness.h"
#include "plexus/testing/mock_policy.h"

#include "support/retire_drain.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

using plexus::testing::harness;

TEST_CASE("mock_retire_quiescence closing a mock channel posts its on_closed exactly once", "[testing][mock][retire_quiescence]")
{
    harness h;
    auto ch             = h.make_channel();
    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });

    ch.close();
    REQUIRE(closes == 0);
    h.drive();
    ch.close();
    h.drive();
    REQUIRE(closes == 1);
}

TEST_CASE("mock_retire_quiescence a handler installed after the mock channel closed and went quiet is still notified", "[testing][mock][retire_quiescence]")
{
    harness h;
    auto ch = h.make_channel();
    ch.close();
    h.drive();

    std::int32_t closes = 0;
    ch.on_closed([&closes] { ++closes; });
    REQUIRE(closes == 0);
    h.drive();
    REQUIRE(closes == 1);
}

TEST_CASE("mock_retire_quiescence a retired mock channel is erased once, after the injection in flight, which it drops", "[testing][mock][retire_quiescence]")
{
    harness h;
    auto ch             = h.make_channel_ptr();
    std::int32_t frames = 0;
    ch->on_data([&frames](std::span<const std::byte>) { ++frames; });
    const std::array<std::byte, 1> payload{std::byte{0x01}};
    ch->inject_inbound(payload);

    const auto outcome = plexus::testing::retire_and_drain(ch, [&h] { h.drive(); });
    REQUIRE(frames == 0);
    REQUIRE(outcome.erased == 1);
    REQUIRE(outcome.empty);
}

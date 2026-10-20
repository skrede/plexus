#include "plexus/testing/harness.h"
#include "plexus/testing/mock_policy.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

using plexus::testing::harness;

TEST_CASE("mock_retire_quiescence closing a mock channel posts its on_closed exactly once", "[testing][mock][retire_quiescence][!shouldfail]")
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

TEST_CASE("mock_retire_quiescence a handler installed after the mock channel closed and went quiet is still notified", "[testing][mock][retire_quiescence][!shouldfail]")
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

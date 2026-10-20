#include "plexus/io/detail/pending_ops.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <stdexcept>

using plexus::io::detail::pending_ops;

TEST_CASE("pending_ops closing with nothing outstanding settles at once", "[io][pending_ops]")
{
    pending_ops ops;
    std::int32_t settled = 0;
    ops.on_settled([&settled] { ++settled; });

    ops.close();
    REQUIRE(ops.settled());
    REQUIRE(settled == 1);
}

TEST_CASE("pending_ops settles on the last end after close and never before close", "[io][pending_ops]")
{
    pending_ops ops;
    std::int32_t settled = 0;
    ops.on_settled([&settled] { ++settled; });

    ops.begin();
    ops.end();
    ops.begin();
    ops.begin();
    ops.close();
    REQUIRE(ops.count() == 2);
    REQUIRE(ops.closing());
    REQUIRE_FALSE(ops.settled());

    ops.end();
    REQUIRE(settled == 0);
    ops.end();
    REQUIRE(ops.settled());
    REQUIRE(settled == 1);
}

TEST_CASE("pending_ops runs its hook exactly once across repeated close and end", "[io][pending_ops]")
{
    pending_ops ops;
    std::int32_t settled = 0;
    ops.on_settled([&settled] { ++settled; });

    ops.close();
    ops.close();
    ops.begin();
    ops.end();
    ops.close();
    REQUIRE(settled == 1);
}

TEST_CASE("pending_ops runs a hook installed after settling at installation", "[io][pending_ops]")
{
    pending_ops ops;
    ops.close();
    REQUIRE(ops.settled());

    std::int32_t settled = 0;
    ops.on_settled([&settled] { ++settled; });
    REQUIRE(settled == 1);
}

TEST_CASE("pending_ops fails closed on an end with nothing outstanding", "[io][pending_ops]")
{
    pending_ops ops;
    REQUIRE_THROWS_AS(ops.end(), std::runtime_error);

    ops.begin();
    ops.end();
    REQUIRE_THROWS_AS(ops.end(), std::runtime_error);
    REQUIRE(ops.count() == 0);
}

#include "test_multipeer_reconnect_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace multipeer_asio_fixture;

TEST_CASE("multipeer asio: one peer crossing a surrender bound is is_dead while every live peer "
          "stays connected over real TCP",
          "[integration][multipeer][asio]")
{
    constexpr int           k_iterations   = 20;
    constexpr std::size_t   k_n            = 3;
    constexpr std::uint32_t k_max_attempts = 3;
    int                     proven         = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        multipeer_net net(k_n, bounded_cfg(k_max_attempts));
        net.pump_until([&] { return net.all_connected(); });
        REQUIRE(net.all_connected());

        // Surrender peer 0: stop its listener so every re-dial is refused, then close
        // its current accepted socket to start the backoff. Each refused re-dial
        // advances slot 0's attempt_count until it crosses the bound and is_dead latches.
        net.peer(0).transport.close();
        net.peer(0).drop();
        net.pump_until([&] { return net.a.is_dead(net.peer(0).id); });
        REQUIRE(net.a.is_dead(net.peer(0).id));

        // Surrender without collateral: every other peer stays connected and not dead.
        net.settle();
        for(std::size_t i = 1; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(!net.a.is_dead(net.peer(i).id));
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

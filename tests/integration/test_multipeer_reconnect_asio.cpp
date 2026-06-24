#include "test_multipeer_reconnect_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace multipeer_asio_fixture;

TEST_CASE("multipeer asio: concurrent real socket closes re-dial each dropped slot independently; "
          "survivors are undisturbed over real TCP",
          "[integration][multipeer][asio]")
{
    constexpr int         k_iterations = 30;
    constexpr std::size_t k_n          = 3;
    constexpr std::size_t k_dropped    = 2;
    int                   proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        multipeer_net net(k_n);
        net.pump_until([&] { return net.all_connected(); });
        REQUIRE(net.all_connected());

        std::array<std::uint64_t, k_n> epoch{};
        std::array<std::uint32_t, k_n> attempts{};
        for(std::size_t i = 0; i < k_n; ++i)
        {
            epoch[i]    = net.a.session_for(net.peer(i).id)->session_id();
            attempts[i] = net.a.attempt_count(net.peer(i).id);
        }

        // Concurrent drop: close K accepted sockets back-to-back. The dialer's read
        // loops surface each FIN -> on_error -> the slot's driver re-dials (production
        // route; no hand-injected established-drop call).
        for(std::size_t i = 0; i < k_dropped; ++i)
            net.peer(i).drop();

        // Each dropped slot re-handshakes a FRESH epoch on its own backoff.
        net.pump_until(
                [&]
                {
                    for(std::size_t i = 0; i < k_dropped; ++i)
                        if(!(net.a.is_connected(net.peer(i).id) && net.a.session_for(net.peer(i).id)->session_id() != epoch[i]))
                            return false;
                    return true;
                });
        net.settle();

        for(std::size_t i = 0; i < k_dropped; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() != epoch[i]);
            REQUIRE(net.a.attempt_count(net.peer(i).id) >= attempts[i] + 1);
        }
        // Survivors: never dropped, so never re-dialed — epoch and attempt_count hold.
        for(std::size_t i = k_dropped; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() == epoch[i]);
            REQUIRE(net.a.attempt_count(net.peer(i).id) == attempts[i]);
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

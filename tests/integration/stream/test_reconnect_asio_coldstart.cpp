#include "test_reconnect_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reconnect_asio_fixture;

TEST_CASE("asio reconnect: a dial to a closed port re-dials until a listener appears, then "
          "completes over real TCP",
          "[integration][reconnect][asio]")
{
    constexpr int k_iterations = 30;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // Bind a listener to learn a free port, then stop it so the port is CLOSED.
        ::asio::io_context probe_io;
        pasio::asio_transport probe{probe_io};
        probe.listen({"tcp", "127.0.0.1:0"});
        const auto port = probe.port();
        probe.close(); // the port is now closed → a dial there is refused

        tcp_reconnect h(fast_cfg(), /*listen_first=*/false, port);
        h.driver->start();
        // The initial dial is refused; the driver schedules a re-dial (attempt advances).
        h.pump_until([&] { return h.driver->attempt_count() >= 1; });
        REQUIRE(h.driver->attempt_count() >= 1);
        REQUIRE(!h.driver->is_surrendered());
        REQUIRE(!h.both_complete());

        // Bring the endpoint up on the SAME port: a subsequent re-dial connects and completes.
        h.transport.listen({"tcp", "127.0.0.1:" + std::to_string(port)});
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

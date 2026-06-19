#include "test_reconnect_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reconnect_unix_fixture;

TEST_CASE("unix reconnect: an established session whose channel drops re-dials and re-handshakes "
          "over real AF_UNIX",
          "[integration][reconnect][unix]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        unix_reconnect h(fast_cfg(), /*listen_first=*/true);
        h.driver->start();
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        const auto first_epoch = h.requester->session_id();

        // Drop the established connection: close the accepted (server) socket. The
        // dialer's read loop surfaces a transport error -> on_error -> the driver
        // re-dials. (A clean tear_down is NOT used — this is a real transport drop.)
        h.resp_ctx.channel->socket().close();
        h.pump_until([&] { return h.drops_seen >= 1; });
        REQUIRE(h.drops_seen >= 1);
        REQUIRE(h.driver->attempt_count() >= 1);

        // The re-dial finds the still-up listener and re-handshakes to a fresh epoch.
        h.pump_until([&] { return h.both_complete() && h.requester->session_id() != first_epoch; });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        REQUIRE(h.requester->session_id() != first_epoch);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

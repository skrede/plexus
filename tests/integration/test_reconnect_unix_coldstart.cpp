#include "test_reconnect_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace reconnect_unix_fixture;

TEST_CASE("unix reconnect: a cold-start dial to a missing socket re-dials until a listener "
          "appears, then completes over real AF_UNIX",
          "[integration][reconnect][unix]")
{
    constexpr int k_iterations = 30;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // No socket file bound yet: the initial dial is refused (ENOENT mapped to a
        // refused/connection error), the driver schedules a re-dial (the same-host
        // demand cold-start race — the demand must be resurrected, not dropped).
        unix_reconnect h(fast_cfg(), /*listen_first=*/false);
        h.driver->start();
        h.pump_until([&] { return h.driver->attempt_count() >= 1; });
        REQUIRE(h.driver->attempt_count() >= 1);
        REQUIRE(!h.driver->is_surrendered());
        REQUIRE(!h.both_complete());

        // Bring the endpoint up on the SAME path: a subsequent re-dial connects and completes.
        h.transport.listen({"unix", h.sock.path});
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#include "test_transport_dial_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace transport_dial_unix_fixture;

TEST_CASE("unix transport: a DIALED peer_session pair completes the handshake over real AF_UNIX "
          "and mints epochs, looped",
          "[integration][transport][unix]")
{
    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_unix_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });

        REQUIRE(l.requester.has_value());
        REQUIRE(l.responder.has_value());
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

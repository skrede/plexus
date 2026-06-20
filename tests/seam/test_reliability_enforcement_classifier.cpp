#include "plexus/io/routing_engine.h"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("reliability enforcement: scheme_is_reliable mirrors the selector's classification",
          "[udp][enforcement][classifier]")
{
    using plexus::io::scheme_is_reliable;
    // udp is best_effort (NOT reliable); udpr/tcp/tls/unix/inproc satisfy reliable; an
    // unknown scheme is fail-closed (not reliable). This mapping MUST stay consistent
    // with the asio selector's reliability_of_scheme.
    REQUIRE_FALSE(scheme_is_reliable("udp"));
    REQUIRE(scheme_is_reliable("udpr"));
    REQUIRE(scheme_is_reliable("tcp"));
    REQUIRE(scheme_is_reliable("tls"));
    REQUIRE(scheme_is_reliable("unix"));
    REQUIRE(scheme_is_reliable("inproc"));
    REQUIRE_FALSE(scheme_is_reliable("ws")); // unknown: fail-closed
}

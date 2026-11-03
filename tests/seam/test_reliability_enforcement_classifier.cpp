#include "plexus/io/routing_engine.h"
#include "plexus/io/transport_selector.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string_view>

TEST_CASE("reliability enforcement: scheme_is_reliable mirrors the selector's classification", "[udp][enforcement][classifier]")
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

TEST_CASE("reliability enforcement: scheme_is_best_effort mirrors the selector's best-effort classification", "[udp][enforcement][classifier]")
{
    using plexus::io::reliability_hint;
    using plexus::io::scheme_is_best_effort;
    const plexus::io::transport_selector selector{};
    constexpr std::array<std::string_view, 9> schemes{"udp", "dtls", "udpr", "tcp", "tls", "unix", "inproc", "serial", "ws"};
    for(const std::string_view s : schemes)
    {
        INFO(s);
        CHECK(scheme_is_best_effort(s) == (selector.reliability_of_scheme(s) == reliability_hint::best_effort));
    }
    CHECK(scheme_is_best_effort("udp"));
    CHECK(scheme_is_best_effort("dtls"));
    CHECK_FALSE(scheme_is_best_effort("udpr"));
}

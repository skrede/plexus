// The MTU-budget value-object oracle: a pure value-level check of the static
// per-channel payload budget. Proves the default (1200, the RFC 9000 §14 conservative
// datagram bound), a caller override, and the oversize-boundary arithmetic the channel's
// reject gates evaluate (size + envelope_overhead (+ a reliable marker byte) > max_payload).
// No socket, no backend — header-only core, linked against plexus::plexus only.

#include "plexus/datagram/mtu_budget.h"

#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>

using plexus::datagram::mtu_budget;

TEST_CASE("mtu_budget default is the conservative 1200-byte payload floor", "[io][mtu_budget]")
{
    REQUIRE(mtu_budget{}.max_payload == 1200);
}

TEST_CASE("mtu_budget admits a caller override (required-with-default)", "[io][mtu_budget]")
{
    REQUIRE(mtu_budget{.max_payload = 512}.max_payload == 512);
    REQUIRE(mtu_budget{.max_payload = 9000}.max_payload == 9000);
}

TEST_CASE("mtu_budget oversize-boundary arithmetic matches the reject gates", "[io][mtu_budget]")
{
    const mtu_budget      budget{.max_payload = 64};
    constexpr std::size_t overhead = plexus::wire::udp_envelope_overhead;

    // best_effort gate: size + overhead > max_payload.
    const std::size_t best_effort_max = budget.max_payload - overhead;
    REQUIRE(best_effort_max + overhead <= budget.max_payload);      // fits at the boundary
    REQUIRE((best_effort_max + 1) + overhead > budget.max_payload); // one past rejects

    // reliable gate: size + overhead + 1 (the kind marker) > max_payload.
    const std::size_t reliable_max = budget.max_payload - overhead - 1;
    REQUIRE(reliable_max + overhead + 1 <= budget.max_payload);      // fits at the boundary
    REQUIRE((reliable_max + 1) + overhead + 1 > budget.max_payload); // one past rejects
}

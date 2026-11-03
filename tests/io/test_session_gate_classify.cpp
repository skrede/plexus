#include "plexus/io/detail/peer_session_gate.h"

#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstdint>
#include <algorithm>

using plexus::io::detail::classify;
using plexus::io::detail::gate_verdict;
namespace wire = plexus::wire;

namespace {

constexpr std::array k_handshake{wire::msg_type::handshake_req, wire::msg_type::handshake_resp};

constexpr std::array k_unused{wire::msg_type::bidirectional, wire::msg_type::fetch_metadata};

constexpr std::array k_secure{
        wire::msg_type::unidirectional, wire::msg_type::subscribe,          wire::msg_type::unsubscribe,         wire::msg_type::fetch_latched,    wire::msg_type::rpc_request,
        wire::msg_type::rpc_response,   wire::msg_type::subscribe_response, wire::msg_type::heartbeat,           wire::msg_type::declare,          wire::msg_type::peer_report,
        wire::msg_type::forwarded,      wire::msg_type::companion_ready,    wire::msg_type::companion_withdrawn, wire::msg_type::companion_switch, wire::msg_type::companion_offer,
};

constexpr std::uint32_t k_first_unknown = 0x14;

constexpr bool listed(wire::msg_type type)
{
    const auto in = [type](const auto &table) { return std::ranges::find(table, type) != table.end(); };
    return in(k_handshake) || in(k_unused) || in(k_secure);
}

constexpr bool tables_cover_the_enumeration()
{
    for(std::uint32_t b = 1; b < k_first_unknown; ++b)
        if(!listed(static_cast<wire::msg_type>(b)))
            return false;
    return k_handshake.size() + k_unused.size() + k_secure.size() == k_first_unknown - 1;
}

static_assert(tables_cover_the_enumeration());

void check_malformed_in_both_states(std::uint32_t byte)
{
    INFO("byte " << byte);
    const auto type = static_cast<wire::msg_type>(byte);
    CHECK(classify(false, type) == gate_verdict::malformed);
    CHECK(classify(true, type) == gate_verdict::malformed);
}

}

TEST_CASE("session gate classify: a complete session admits every type but bidirectional and fetch_metadata", "[io][session_gate]")
{
    for(const wire::msg_type type : k_handshake)
        CHECK(classify(true, type) == gate_verdict::admit);
    for(const wire::msg_type type : k_secure)
        CHECK(classify(true, type) == gate_verdict::admit);
    for(const wire::msg_type type : k_unused)
        CHECK(classify(true, type) == gate_verdict::malformed);
}

TEST_CASE("session gate classify: before completion only the handshake types admit", "[io][session_gate]")
{
    for(const wire::msg_type type : k_handshake)
        CHECK(classify(false, type) == gate_verdict::admit);
    for(const wire::msg_type type : k_secure)
        CHECK(classify(false, type) == gate_verdict::unestablished);
    for(const wire::msg_type type : k_unused)
        CHECK(classify(false, type) == gate_verdict::malformed);
}

TEST_CASE("session gate classify: a byte outside the enumeration is malformed in both states", "[io][session_gate]")
{
    check_malformed_in_both_states(0x00);
    for(std::uint32_t b = k_first_unknown; b <= 0xFF; ++b)
        check_malformed_in_both_states(b);
}

TEST_CASE("session gate classify: the verdict is a constant expression", "[io][session_gate]")
{
    STATIC_REQUIRE(classify(false, wire::msg_type::handshake_req) == gate_verdict::admit);
    STATIC_REQUIRE(classify(false, wire::msg_type::rpc_request) == gate_verdict::unestablished);
    STATIC_REQUIRE(classify(true, static_cast<wire::msg_type>(0xFF)) == gate_verdict::malformed);
}

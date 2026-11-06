#include "plexus/wire/subscribe.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::wire;

namespace {

std::vector<std::byte> response_of(subscribe_status status)
{
    return encode_subscribe_response(subscribe_response{.topic_hash = 0xDEADBEEFCAFEF00DULL, .status = status, .offer = offer_state::offered});
}

std::vector<std::byte> response_with_status_byte(std::uint8_t status)
{
    std::vector<std::byte> payload = response_of(subscribe_status::subscribed);
    payload[8]                     = std::byte{status};
    return payload;
}

}

TEST_CASE("subscribe status: an unknown status byte decodes to nullopt", "[wire][subscribe_status]")
{
    for(const std::uint8_t status : std::array<std::uint8_t, 3>{0x0A, 0x7F, 0xFF})
    {
        INFO("status byte " << static_cast<std::uint32_t>(status));
        CHECK_FALSE(decode_subscribe_response(response_with_status_byte(status)).has_value());
    }
}

TEST_CASE("subscribe status: a status byte of zero decodes to nullopt", "[wire][subscribe_status]")
{
    CHECK_FALSE(decode_subscribe_response(response_with_status_byte(0x00)).has_value());
}

TEST_CASE("subscribe status: every wire status round-trips and the local-only one does not", "[wire][subscribe_status]")
{
    constexpr std::array k_wire_statuses{subscribe_status::subscribed,          subscribe_status::created,          subscribe_status::type_mismatch,
                                         subscribe_status::already_subscribed,  subscribe_status::incompatible_qos, subscribe_status::source_identity_incompatible,
                                         subscribe_status::subscribed_degraded, subscribe_status::type_undeclared};
    for(const subscribe_status status : k_wire_statuses)
    {
        const auto decoded = decode_subscribe_response(response_of(status));
        INFO("status byte " << static_cast<std::uint32_t>(status));
        REQUIRE(decoded.has_value());
        CHECK(decoded->status == status);
    }
    CHECK_FALSE(decode_subscribe_response(response_of(subscribe_status::unacknowledged)).has_value());
}

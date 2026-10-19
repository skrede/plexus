#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::wire;

TEST_CASE("companion the verbs take the next free message types", "[wire][companion]")
{
    CHECK(static_cast<std::uint8_t>(msg_type::companion_ready) == 0x10);
    CHECK(static_cast<std::uint8_t>(msg_type::companion_withdrawn) == 0x11);
    CHECK(static_cast<std::uint8_t>(msg_type::companion_switch) == 0x12);
}

TEST_CASE("companion a signal round-trips its topic hash through eight bytes", "[wire][companion]")
{
    const std::array<std::byte, companion_signal_size> bytes = encode_companion({0x0123456789ABCDEFULL});
    const auto decoded                                       = decode_companion(bytes);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0x0123456789ABCDEFULL);
    CHECK(bytes.size() == 8);
}

TEST_CASE("companion a truncated or overlong payload is refused", "[wire][companion]")
{
    const std::array<std::byte, companion_signal_size> bytes = encode_companion({42});
    const std::vector<std::byte> longer(companion_signal_size + 1);

    for(std::size_t size = 0; size < companion_signal_size; ++size)
        CHECK_FALSE(decode_companion(std::span<const std::byte>{bytes.data(), size}).has_value());
    CHECK_FALSE(decode_companion(longer).has_value());
}

TEST_CASE("companion a ready round-trips its topic hash and reader slot through twelve bytes", "[wire][companion]")
{
    const std::array<std::byte, companion_ready_size> bytes = encode_companion_ready({0x0123456789ABCDEFULL, 0xA5C3E1F7u});
    const auto decoded                                      = decode_companion_ready(bytes);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0x0123456789ABCDEFULL);
    CHECK(decoded->reader == 0xA5C3E1F7u);
    CHECK(bytes.size() == 12);
}

TEST_CASE("companion a switch round-trips its topic hash and first sequence through sixteen bytes", "[wire][companion]")
{
    const std::array<std::byte, companion_switch_size> bytes = encode_companion_switch({0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL});
    const auto decoded                                       = decode_companion_switch(bytes);

    REQUIRE(decoded.has_value());
    CHECK(decoded->topic_hash == 0x0123456789ABCDEFULL);
    CHECK(decoded->first_sequence == 0xFEDCBA9876543210ULL);
    CHECK(bytes.size() == 16);
}

TEST_CASE("companion a ready or switch payload of any other length is refused", "[wire][companion]")
{
    const std::vector<std::byte> bytes(companion_switch_size + 1);

    for(std::size_t size = 0; size <= bytes.size(); ++size)
    {
        const std::span<const std::byte> payload{bytes.data(), size};
        CHECK(decode_companion_ready(payload).has_value() == (size == companion_ready_size));
        CHECK(decode_companion_switch(payload).has_value() == (size == companion_switch_size));
    }
}

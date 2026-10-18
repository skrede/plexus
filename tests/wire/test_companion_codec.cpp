#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::wire;

TEST_CASE("companion the two verbs take the next free message types", "[wire][companion]")
{
    CHECK(static_cast<std::uint8_t>(msg_type::companion_ready) == 0x10);
    CHECK(static_cast<std::uint8_t>(msg_type::companion_withdrawn) == 0x11);
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

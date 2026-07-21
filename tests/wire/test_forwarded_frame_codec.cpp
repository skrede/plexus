// The forwarded-frame codec oracle: an untrusted-input decoder for a relayed session verb that
// carries {origin, destination, hop, seq, flags} plus a complete inner frame. It proves the
// byte-exact round-trip (fixed preamble + a u32-length-prefixed inner region), pins the wire layout
// offset-by-offset, and proves the hardened-decode set — a truncated frame or an inner-length prefix
// past the payload remainder yields nullopt with no copy past the payload.

#include "plexus/wire/frame.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::wire::forwarded_frame;
using plexus::wire::encode_forwarded_frame;
using plexus::wire::decode_forwarded_frame;
using plexus::wire::k_forwarded_relay_consent_flag;

namespace {

node_id fill(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

forwarded_frame make_sample()
{
    forwarded_frame ff;
    ff.origin      = fill(0xB0);
    ff.destination = fill(0xC0);
    ff.hop         = 7;
    ff.seq         = 0xABCD;
    ff.flags       = 0;
    ff.inner       = {std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
    return ff;
}

}

TEST_CASE("forwarded_frame codec: the envelope and inner bytes round-trip byte-exactly", "[wire][forwarded]")
{
    const auto ff    = make_sample();
    const auto bytes = encode_forwarded_frame(ff);

    // Wire layout pin: origin(16) + destination(16) + hop(1) + seq(2) + flags(1) + inner_len(4) + inner.
    REQUIRE(bytes.size() == 36 + 4 + 3);
    for(std::size_t i = 0; i < 16; ++i)
        REQUIRE(bytes[i] == static_cast<std::byte>(0xB0 + i));
    for(std::size_t i = 0; i < 16; ++i)
        REQUIRE(bytes[16 + i] == static_cast<std::byte>(0xC0 + i));
    REQUIRE(bytes[32] == std::byte{0x07});
    REQUIRE(bytes[33] == std::byte{0xAB});
    REQUIRE(bytes[34] == std::byte{0xCD});
    REQUIRE(bytes[35] == std::byte{0x00});
    REQUIRE(bytes[36] == std::byte{0x00});
    REQUIRE(bytes[37] == std::byte{0x00});
    REQUIRE(bytes[38] == std::byte{0x00});
    REQUIRE(bytes[39] == std::byte{0x03});
    REQUIRE(bytes[40] == std::byte{0x11});
    REQUIRE(bytes[41] == std::byte{0x22});
    REQUIRE(bytes[42] == std::byte{0x33});

    const auto decoded = decode_forwarded_frame(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == ff);
}

TEST_CASE("forwarded_frame codec: an empty inner region round-trips", "[wire][forwarded]")
{
    auto ff  = make_sample();
    ff.inner.clear();

    const auto bytes   = encode_forwarded_frame(ff);
    REQUIRE(bytes.size() == 36 + 4);
    const auto decoded = decode_forwarded_frame(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->inner.empty());
    REQUIRE(*decoded == ff);
}

TEST_CASE("forwarded_frame codec: the relay-consent flag round-trips", "[wire][forwarded]")
{
    auto ff   = make_sample();
    ff.flags |= k_forwarded_relay_consent_flag;

    const auto decoded = decode_forwarded_frame(encode_forwarded_frame(ff));
    REQUIRE(decoded.has_value());
    REQUIRE((decoded->flags & k_forwarded_relay_consent_flag) != 0);
    REQUIRE(*decoded == ff);
}

TEST_CASE("forwarded_frame codec: a full inner frame round-trips header-on", "[wire][forwarded]")
{
    auto ff = make_sample();
    ff.inner.resize(200);
    for(std::size_t i = 0; i < ff.inner.size(); ++i)
        ff.inner[i] = static_cast<std::byte>(i & 0xFF);

    const auto decoded = decode_forwarded_frame(encode_forwarded_frame(ff));
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->inner.size() == 200);
    REQUIRE(*decoded == ff);
}

TEST_CASE("forwarded_frame codec: every truncation of a full frame decodes to nullopt", "[wire][forwarded]")
{
    const auto bytes = encode_forwarded_frame(make_sample());
    for(std::size_t n = 0; n < bytes.size(); ++n)
        REQUIRE(!decode_forwarded_frame(std::span<const std::byte>{bytes}.first(n)).has_value());
}

TEST_CASE("forwarded_frame codec: an empty span decodes to nullopt", "[wire][forwarded]")
{
    REQUIRE(!decode_forwarded_frame(std::span<const std::byte>{}).has_value());
}

TEST_CASE("forwarded_frame codec: an inner-length prefix past the payload remainder decodes to nullopt", "[wire][forwarded]")
{
    auto bytes = encode_forwarded_frame(make_sample());
    // Overwrite the u32 inner-length prefix (offset 36..39) with a value exceeding the remainder.
    bytes[36] = std::byte{0xFF};
    bytes[37] = std::byte{0xFF};
    bytes[38] = std::byte{0xFF};
    bytes[39] = std::byte{0xFF};
    REQUIRE(!decode_forwarded_frame(bytes).has_value());
}

TEST_CASE("forwarded_frame codec: the msg_type slot is additive (0x0F after peer_report 0x0E)", "[wire][forwarded]")
{
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::forwarded) == 0x0F);
    REQUIRE(static_cast<std::uint8_t>(plexus::wire::msg_type::peer_report) == 0x0E);
}

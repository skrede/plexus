// The discovery announcement codec oracle: an untrusted-input decoder over an open
// multicast group. It proves the round-trip identity (every field + the listens in order),
// the goodbye-flag carry, the malformed-reject set (bad magic, mid-node_id and mid-listens
// truncation, an overrun transport length-prefix, an empty span — each nullopt with no
// partial struct), and the reserved-universe forward-compat (a non-zero universe still
// decodes, read + ignore). These cases double as the codec's fuzz oracle.

#include "plexus/wire/announcement.h"
#include "plexus/wire/byte_order.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::wire::announcement;
using plexus::wire::encode_announcement;
using plexus::wire::decode_announcement;
using plexus::wire::k_announcement_goodbye_flag;
using plexus::wire::k_announcement_magic;

namespace {

node_id make_node_id()
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(0xA0 + i);
    return id;
}

announcement make_sample()
{
    announcement ann;
    ann.node_id  = make_node_id();
    ann.ttl_secs = 30;
    ann.flags    = 0;
    ann.universe = 0;
    ann.listens  = {{"tcp", std::uint16_t{5000}}, {"udp", std::uint16_t{5001}}};
    return ann;
}

}

TEST_CASE("announcement codec: encode then decode round-trips every field in order", "[wire][announcement][discovery]")
{
    const auto ann = make_sample();
    const auto bytes = encode_announcement(ann);

    const auto decoded = decode_announcement(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(*decoded == ann);
    REQUIRE(decoded->listens.size() == 2);
    REQUIRE(decoded->listens[0] == std::pair<std::string, std::uint16_t>{"tcp", std::uint16_t{5000}});
    REQUIRE(decoded->listens[1] == std::pair<std::string, std::uint16_t>{"udp", std::uint16_t{5001}});
}

TEST_CASE("announcement codec: the goodbye flag round-trips set", "[wire][announcement][discovery]")
{
    auto ann   = make_sample();
    ann.flags |= k_announcement_goodbye_flag;

    const auto decoded = decode_announcement(encode_announcement(ann));
    REQUIRE(decoded.has_value());
    REQUIRE((decoded->flags & k_announcement_goodbye_flag) != 0);
    REQUIRE(*decoded == ann);
}

TEST_CASE("announcement codec: a flipped magic decodes to nullopt", "[wire][announcement][discovery]")
{
    auto bytes = encode_announcement(make_sample());
    bytes[0]   = static_cast<std::byte>(std::to_integer<std::uint8_t>(bytes[0]) ^ 0xFF);
    REQUIRE(!decode_announcement(bytes).has_value());
}

TEST_CASE("announcement codec: truncation mid-node_id decodes to nullopt", "[wire][announcement][discovery]")
{
    auto bytes = encode_announcement(make_sample());
    // magic(4) + version(1) + flags(1) + universe(4) = 10; cut into the 16-byte node_id.
    bytes.resize(10 + 8);
    REQUIRE(!decode_announcement(bytes).has_value());
}

TEST_CASE("announcement codec: truncation mid-listens decodes to nullopt", "[wire][announcement][discovery]")
{
    auto bytes = encode_announcement(make_sample());
    // Drop the trailing bytes of the listens block (the last port + part of the last entry).
    bytes.resize(bytes.size() - 4);
    REQUIRE(!decode_announcement(bytes).has_value());
}

TEST_CASE("announcement codec: an overrun transport length-prefix decodes to nullopt", "[wire][announcement][discovery]")
{
    auto bytes = encode_announcement(make_sample());
    // The first transport's u8 length-prefix sits right after n_listens. Offsets:
    // magic(4)+ver(1)+flags(1)+universe(4)+node_id(16)=26, then ttl varint (30 -> 1 byte) at
    // 26, n_listens u8 at 27, the first transport length-prefix at 28. Overstate it so the
    // payload runs past the span; length_prefixed must latch the reader to nullopt.
    bytes[28] = std::byte{0xFF};
    REQUIRE(!decode_announcement(bytes).has_value());
}

TEST_CASE("announcement codec: an empty span decodes to nullopt", "[wire][announcement][discovery]")
{
    REQUIRE(!decode_announcement(std::span<const std::byte>{}).has_value());
}

TEST_CASE("announcement codec: a struct-level universe round-trips", "[wire][announcement][discovery]")
{
    auto ann      = make_sample();
    ann.universe  = 0;
    const auto rt = decode_announcement(encode_announcement(ann));
    REQUIRE(rt.has_value());
    REQUIRE(rt->universe == 0);
}

TEST_CASE("announcement codec: a non-zero universe still decodes (wire carries it, decode never value-rejects)", "[wire][announcement][discovery]")
{
    // Hand-build a buffer with a non-zero universe u32: the wire carries any value losslessly and
    // decode never value-rejects — the gating on the value is the discovery leaf's, tested there.
    auto bytes = encode_announcement(make_sample());
    // universe u32 sits at offset magic(4)+version(1)+flags(1) = 6.
    plexus::wire::detail::write_u32(bytes.data() + 6, 0xDEADBEEFu);

    const auto decoded = decode_announcement(bytes);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->universe == 0xDEADBEEFu);
}

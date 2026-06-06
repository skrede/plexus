// The reliable-ARQ control-frame oracle: the cumulative+selective ack codec and the
// segment-marker wrapper round-trip exactly, and a short/malformed/mis-marked buffer
// decodes fail-closed to nullopt. The leading control discriminator is what keeps a
// data segment, an ack, and a handshake frame on ONE inbound demux path without
// aliasing — so the marker spaces are asserted disjoint here too. Pure header-only
// wire units (no asio backend): links only plexus::wire.

#include "plexus/wire/udp_ack.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace wire = plexus::wire;

namespace {

std::vector<std::byte> bytes_of(std::initializer_list<std::uint8_t> vals)
{
    std::vector<std::byte> out;
    out.reserve(vals.size());
    for(auto v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

TEST_CASE("udp ack: a cumulative + selective ack round-trips exactly", "[udp][ack]")
{
    wire::udp_ack ack;
    ack.cumulative = 0xBEEF;
    ack.mark_hole(0);
    ack.mark_hole(5);
    ack.mark_hole(63);
    ack.mark_hole(wire::udp_ack::bitmap_bits - 1);   // the last nameable hole

    std::vector<std::byte> buf;
    wire::encode_udp_ack_into(buf, ack);
    REQUIRE(buf.size() == wire::udp_ack_frame_size);

    auto dec = wire::decode_udp_ack(buf);
    REQUIRE(dec.has_value());
    REQUIRE(dec->cumulative == 0xBEEF);
    REQUIRE(dec->hole_received(0));
    REQUIRE(dec->hole_received(5));
    REQUIRE(dec->hole_received(63));
    REQUIRE(dec->hole_received(wire::udp_ack::bitmap_bits - 1));
    REQUIRE_FALSE(dec->hole_received(1));
    REQUIRE_FALSE(dec->hole_received(6));
    REQUIRE_FALSE(dec->hole_received(62));
}

TEST_CASE("udp ack: a short or mis-marked buffer decodes fail-closed", "[udp][ack]")
{
    // A buffer shorter than the fixed ack size: nullopt (never read past it).
    auto too_short = bytes_of({static_cast<std::uint8_t>(wire::udp_arq_kind::ack), 0x00});
    REQUIRE_FALSE(wire::decode_udp_ack(too_short).has_value());

    // The right size but the wrong leading marker (a segment, not an ack): nullopt.
    std::vector<std::byte> wrong_marker(wire::udp_ack_frame_size, std::byte{0});
    wrong_marker[0] = static_cast<std::byte>(wire::udp_arq_kind::segment);
    REQUIRE_FALSE(wire::decode_udp_ack(wrong_marker).has_value());

    // An empty buffer: nullopt.
    REQUIRE_FALSE(wire::decode_udp_ack({}).has_value());
}

TEST_CASE("udp segment: the marker wraps and strips a payload round-trip", "[udp][ack]")
{
    auto payload = bytes_of({0x01, 0x02, 0x03, 0xFF});
    std::vector<std::byte> framed;
    wire::encode_udp_segment_into(framed, payload);
    REQUIRE(framed.size() == payload.size() + 1);

    auto inner = wire::decode_udp_segment(framed);
    REQUIRE(inner.has_value());
    REQUIRE(inner->size() == payload.size());
    REQUIRE(std::equal(inner->begin(), inner->end(), payload.begin()));

    // An empty payload still frames to a single marker byte and strips to an empty span.
    std::vector<std::byte> empty_framed;
    wire::encode_udp_segment_into(empty_framed, {});
    REQUIRE(empty_framed.size() == 1);
    auto empty_inner = wire::decode_udp_segment(empty_framed);
    REQUIRE(empty_inner.has_value());
    REQUIRE(empty_inner->empty());
}

TEST_CASE("udp arq demux: peek classifies segment vs ack and rejects unknown/handshake markers",
          "[udp][ack]")
{
    std::vector<std::byte> seg;
    wire::encode_udp_segment_into(seg, bytes_of({0xAA}));
    REQUIRE(wire::peek_udp_arq_kind(seg) == wire::udp_arq_kind::segment);

    wire::udp_ack ack;
    std::vector<std::byte> ackbuf;
    wire::encode_udp_ack_into(ackbuf, ack);
    REQUIRE(wire::peek_udp_arq_kind(ackbuf) == wire::udp_arq_kind::ack);

    // The handshake markers (request=0, response=1) are NOT ARQ markers: a kind=1
    // inner frame leading with 0 or 1 is a handshake, never misclassified as ARQ.
    REQUIRE_FALSE(wire::peek_udp_arq_kind(bytes_of({0})).has_value());
    REQUIRE_FALSE(wire::peek_udp_arq_kind(bytes_of({1})).has_value());
    REQUIRE_FALSE(wire::peek_udp_arq_kind({}).has_value());
}

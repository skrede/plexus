// The UDP envelope codec oracle: the [ver_flags][uint16 seq][frame] outer wrapper
// round-trips an opaque inner frame byte-identically, sizes the seq for the full
// uint16 range (big-endian on the wire), exposes the kind discriminator, reserves
// the FRAGMENTED bit (decoded, never set this phase), and fail-closes a datagram
// shorter than the fixed overhead. A pure header-only unit — no backend.

#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

using plexus::wire::udp_envelope_kind;
using plexus::wire::udp_envelope_overhead;
using plexus::wire::wrap_udp;
using plexus::wire::wrap_udp_into;
using plexus::wire::unwrap_udp;

namespace {

std::vector<std::byte> bytes(std::initializer_list<unsigned> vals)
{
    std::vector<std::byte> out;
    out.reserve(vals.size());
    for(auto v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

bool spans_equal(std::span<const std::byte> a, std::span<const std::byte> b)
{
    if(a.size() != b.size())
        return false;
    for(std::size_t i = 0; i < a.size(); ++i)
        if(a[i] != b[i])
            return false;
    return true;
}

}

TEST_CASE("udp envelope: best_effort overhead is exactly 3 bytes", "[udp][envelope]")
{
    REQUIRE(udp_envelope_overhead == 3u);

    const auto frame = bytes({0xAA, 0xBB, 0xCC, 0xDD});
    const auto wire  = wrap_udp(udp_envelope_kind::best_effort, 42, frame);
    REQUIRE(wire.size() == udp_envelope_overhead + frame.size());
}

TEST_CASE("udp envelope: wrap/unwrap round-trips kind, seq, and the inner frame byte-identically", "[udp][envelope]")
{
    const auto frame = bytes({0x01, 0x02, 0x03, 0x04, 0x05});
    const auto wire  = wrap_udp(udp_envelope_kind::best_effort, 1234, frame);

    auto dec = unwrap_udp(wire);
    REQUIRE(dec.has_value());
    REQUIRE(dec->kind == udp_envelope_kind::best_effort);
    REQUIRE(dec->seq == 1234u);
    REQUIRE_FALSE(dec->fragmented);
    REQUIRE(spans_equal(dec->frame, frame));
}

TEST_CASE("udp envelope: an empty inner frame round-trips (header only)", "[udp][envelope]")
{
    const auto wire = wrap_udp(udp_envelope_kind::best_effort, 7, {});
    REQUIRE(wire.size() == udp_envelope_overhead);

    auto dec = unwrap_udp(wire);
    REQUIRE(dec.has_value());
    REQUIRE(dec->frame.empty());
    REQUIRE(dec->seq == 7u);
}

TEST_CASE("udp envelope: seq round-trips across the full uint16 range, big-endian on the wire", "[udp][envelope]")
{
    const auto frame = bytes({0x99});
    for(std::uint16_t seq : {std::uint16_t{0}, std::uint16_t{1}, std::uint16_t{255}, std::uint16_t{256}, std::uint16_t{32768}, std::uint16_t{65535}})
    {
        const auto wire = wrap_udp(udp_envelope_kind::best_effort, seq, frame);
        // Big-endian: seq high byte at offset 1, low byte at offset 2.
        REQUIRE(static_cast<unsigned>(wire[1]) == (seq >> 8));
        REQUIRE(static_cast<unsigned>(wire[2]) == (seq & 0xFFu));

        auto dec = unwrap_udp(wire);
        REQUIRE(dec.has_value());
        REQUIRE(dec->seq == seq);
    }
}

TEST_CASE("udp envelope: the kind discriminator distinguishes best_effort from reliable-ARQ", "[udp][envelope]")
{
    const auto frame = bytes({0x11, 0x22});

    auto be  = unwrap_udp(wrap_udp(udp_envelope_kind::best_effort, 5, frame));
    auto rel = unwrap_udp(wrap_udp(udp_envelope_kind::reliable_arq, 5, frame));
    REQUIRE(be.has_value());
    REQUIRE(rel.has_value());
    REQUIRE(be->kind == udp_envelope_kind::best_effort);
    REQUIRE(rel->kind == udp_envelope_kind::reliable_arq);

    // kind lives in bits 7..6: best_effort = 0x00, reliable-ARQ = 0x40.
    const auto rel_wire = wrap_udp(udp_envelope_kind::reliable_arq, 5, frame);
    REQUIRE(static_cast<unsigned>(rel_wire[0]) == 0x40u);
    const auto be_wire = wrap_udp(udp_envelope_kind::best_effort, 5, frame);
    REQUIRE(static_cast<unsigned>(be_wire[0]) == 0x00u);
}

TEST_CASE("udp envelope: the FRAGMENTED bit is never set on encode this phase", "[udp][envelope]")
{
    const auto frame    = bytes({0x33});
    const auto be_wire  = wrap_udp(udp_envelope_kind::best_effort, 9, frame);
    const auto rel_wire = wrap_udp(udp_envelope_kind::reliable_arq, 9, frame);
    // bit0 (FRAGMENTED) reserved 0; bits 5..1 reserved 0 — only the kind bits carry.
    REQUIRE((static_cast<unsigned>(be_wire[0]) & 0x01u) == 0u);
    REQUIRE((static_cast<unsigned>(rel_wire[0]) & 0x3Fu) == 0u);
}

TEST_CASE("udp envelope: a datagram with the FRAGMENTED bit set decodes (reserved, not rejected)", "[udp][envelope]")
{
    // Hand-craft a datagram with kind=0 and the FRAGMENTED bit set, plus a 2-byte frame.
    const auto wire = bytes({0x01, 0x00, 0x2A, 0xDE, 0xAD});
    auto       dec  = unwrap_udp(wire);
    REQUIRE(dec.has_value());
    REQUIRE(dec->kind == udp_envelope_kind::best_effort);
    REQUIRE(dec->fragmented);
    REQUIRE(dec->seq == 0x2Au);
    REQUIRE(dec->frame.size() == 2u);
}

TEST_CASE("udp envelope: a buffer shorter than the overhead unwraps to nullopt (fail-closed)", "[udp][envelope]")
{
    REQUIRE_FALSE(unwrap_udp(bytes({})).has_value());
    REQUIRE_FALSE(unwrap_udp(bytes({0x00})).has_value());
    REQUIRE_FALSE(unwrap_udp(bytes({0x00, 0x00})).has_value());
    // Exactly the overhead with an empty frame is valid (the boundary).
    REQUIRE(unwrap_udp(bytes({0x00, 0x00, 0x00})).has_value());
}

TEST_CASE("udp envelope: wrap_udp_into reuses the buffer and matches the allocating overload", "[udp][envelope]")
{
    const auto             frame = bytes({0xCA, 0xFE, 0xBA, 0xBE});
    std::vector<std::byte> scratch;
    wrap_udp_into(scratch, udp_envelope_kind::reliable_arq, 4096, frame);

    const auto one_shot = wrap_udp(udp_envelope_kind::reliable_arq, 4096, frame);
    REQUIRE(spans_equal(scratch, one_shot));

    auto dec = unwrap_udp(scratch);
    REQUIRE(dec.has_value());
    REQUIRE(dec->kind == udp_envelope_kind::reliable_arq);
    REQUIRE(dec->seq == 4096u);
    REQUIRE(spans_equal(dec->frame, frame));
}

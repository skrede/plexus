#include "plexus/wire/crc32c.h"
#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

// Known-answer vectors that pin the EXACT CRC-32C (Castagnoli) bytes the wire
// layer produces for the serial integrity trailer. The serial link appends a
// CRC32C over each frame; a future bare-metal build runs the SAME header-only
// crc32c.h, so these literals are the cross-build contract. If a target ever
// diverged (a different table, poly, or seed), every serial frame would fail its
// trailer check and the link would silently reject all traffic — these
// assertions surface that divergence at unit-test time, not on-bench.
//
// The expected u32s are captured constants (computed once during authoring and
// written as hex literals), deliberately NOT recomputed via a second crc32c call
// inside the assertion: a typo in the routine must fail the KAT, not pass itself.

using namespace plexus::wire;

namespace {

std::span<const std::byte> as_bytes(std::string_view s) noexcept
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

}

TEST_CASE("crc32c kat: the canonical Castagnoli check vector", "[wire][crc32c][kat]")
{
    // The Castagnoli check value (CRC-32C of the ASCII "123456789"), the vector
    // every CRC-32C implementation publishes (RFC 3720 / iSCSI / Koopman). A
    // host and a future MCU build must both produce this exact word.
    REQUIRE(crc32c(as_bytes("123456789")) == 0xE3069283u);
}

TEST_CASE("crc32c kat: the empty-input identity", "[wire][crc32c][kat]")
{
    // The empty span is the seed-chaining identity element: 0x00000000.
    REQUIRE(crc32c({}) == 0x00000000u);
}

TEST_CASE("crc32c kat: the all-zero serial-frame header", "[wire][crc32c][kat]")
{
    // The 28-byte all-zero header is the most common frame prefix shape; its
    // CRC32C is pinned so the host==MCU equivalence over the header region is
    // proven byte-equal independent of any payload.
    std::array<std::byte, header_size> zero_header{};
    REQUIRE(crc32c(std::span<const std::byte>{zero_header}) == 0xF7C9C769u);
}

TEST_CASE("crc32c kat: a representative serial frame", "[wire][crc32c][kat]")
{
    // A fixed, fully-specified frame: a real encode_header (big-endian, magic
    // 0x56 0x50) over a non-zero session_id with a short ASCII payload appended.
    // Both the header-only CRC and the end-to-end [header||payload] CRC are
    // pinned to exact words the MCU build must reproduce.
    const auto payload = as_bytes("plexus-serial-frame");

    frame_header hdr{};
    hdr.type         = msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = 1;
    hdr.timestamp_ns = 0;
    hdr.payload_len  = payload.size();

    const auto header_bytes = encode_header(hdr);
    REQUIRE(crc32c(std::span<const std::byte>{header_bytes}) == 0x6FFA4779u);

    std::vector<std::byte> frame;
    frame.insert(frame.end(), header_bytes.begin(), header_bytes.end());
    frame.insert(frame.end(), payload.begin(), payload.end());
    REQUIRE(crc32c(std::span<const std::byte>{frame}) == 0x8E2BC0E4u);
}

TEST_CASE("crc32c kat: seed-chaining a serial frame byte-equal to the one-shot", "[wire][crc32c][kat]")
{
    // The trailer decorator computes the CRC over two chunks (header then
    // payload) without concatenating them. This proves that zero-copy chained
    // computation is byte-identical to the one-shot over [header||payload].
    const auto payload = as_bytes("plexus-serial-frame");

    frame_header hdr{};
    hdr.type         = msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = 1;
    hdr.timestamp_ns = 0;
    hdr.payload_len  = payload.size();

    const auto header_bytes = encode_header(hdr);

    std::vector<std::byte> frame;
    frame.insert(frame.end(), header_bytes.begin(), header_bytes.end());
    frame.insert(frame.end(), payload.begin(), payload.end());

    const std::uint32_t one_shot = crc32c(std::span<const std::byte>{frame});
    const std::uint32_t chained  = crc32c(payload, crc32c(std::span<const std::byte>{header_bytes}));

    REQUIRE(chained == one_shot);
    REQUIRE(chained == 0x8E2BC0E4u);
}

// The read-buffer partial-read oracle: one framed wire message fed to the
// frame_reassembler in many small chunks — split below the fixed header, split
// mid-payload, and one byte at a time — still produces EXACTLY one complete_frame whose
// payload is the byte-identical full header-on frame. The reassembler accumulates partial
// reads across feeds and emits a frame only once the full header-plus-payload is buffered,
// so the oracle pins the accumulation behavior any later read-buffer resize must preserve.

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame_reassembler.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus;

namespace {

std::vector<std::byte> make_payload(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 31u + 7u) & 0xFFu);
    return v;
}

std::vector<std::byte> framed(const std::vector<std::byte> &payload)
{
    wire::frame_header hdr{};
    hdr.type = wire::msg_type::unidirectional;
    hdr.flags = 0;
    hdr.session_id = 1;
    hdr.timestamp_ns = 0;
    hdr.payload_len = payload.size();
    return wire::encode_frame(hdr, payload);
}

// Feed `wire` to `r` in fixed-size chunks; return every complete_frame that emerged
// across the whole split.
std::vector<wire::complete_frame> feed_in_chunks(wire::frame_reassembler &r,
                                                 std::span<const std::byte> bytes, std::size_t chunk)
{
    std::vector<wire::complete_frame> out;
    for(std::size_t off = 0; off < bytes.size(); off += chunk)
    {
        const auto len = std::min(chunk, bytes.size() - off);
        auto result = r.feed(bytes.subspan(off, len));
        REQUIRE(result.error == wire::feed_error::none);
        for(auto &f : result.frames)
            out.push_back(std::move(f));
    }
    return out;
}

}

TEST_CASE("one framed message split one byte at a time reassembles to a single byte-identical frame",
          "[integration][partial-read][copy]")
{
    const auto payload = make_payload(300);
    const auto wire_bytes = framed(payload);

    wire::frame_reassembler r;
    auto frames = feed_in_chunks(r, wire_bytes, 1);   // sub-header AND mid-payload splits

    REQUIRE(frames.size() == 1);
    REQUIRE(frames.front().payload.size() == wire_bytes.size());
    CHECK(frames.front().payload == std::span<const std::byte>(wire_bytes));
    CHECK_FALSE(r.frame_in_progress());
}

TEST_CASE("a framed message split at the header/payload boundary reassembles intact",
          "[integration][partial-read][copy]")
{
    const auto payload = make_payload(500);
    const auto wire_bytes = framed(payload);

    wire::frame_reassembler r;

    // Split mid-header: the reassembler holds the partial header and makes no frame yet.
    auto first = r.feed(std::span<const std::byte>(wire_bytes).subspan(0, wire::header_size - 3));
    REQUIRE(first.error == wire::feed_error::none);
    CHECK(first.frames.empty());
    CHECK(r.frame_in_progress());

    // Complete the header and add a slice of the payload: still no frame (payload short).
    auto second = r.feed(std::span<const std::byte>(wire_bytes).subspan(wire::header_size - 3, 10));
    REQUIRE(second.error == wire::feed_error::none);
    CHECK(second.frames.empty());

    // Feed the remaining payload bytes: the single frame now emerges intact.
    const std::size_t fed = wire::header_size - 3 + 10;
    auto third = r.feed(std::span<const std::byte>(wire_bytes).subspan(fed));
    REQUIRE(third.error == wire::feed_error::none);
    REQUIRE(third.frames.size() == 1);
    REQUIRE(third.frames.front().payload.size() == wire_bytes.size());
    CHECK(third.frames.front().payload == std::span<const std::byte>(wire_bytes));
    CHECK_FALSE(r.frame_in_progress());
}

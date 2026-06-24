#include "test_recording_channel_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recording_channel_fixture;

TEST_CASE("recording_channel forwards send bytes verbatim and taps the OUT frame", "[recording_channel][wire]")
{
    auto                         *raw = new test_lower;
    recording_channel<test_lower> ch{std::unique_ptr<test_lower>(raw)};

    std::vector<std::tuple<wire_direction, std::uint64_t, std::vector<std::byte>>> taps;
    ch.on_wire([&](wire_direction dir, std::uint64_t seq, std::span<const std::byte> b) { taps.emplace_back(dir, seq, std::vector<std::byte>(b.begin(), b.end())); });

    const auto frame = blob(0x10, 64);
    ch.send(std::span<const std::byte>{frame});

    // Lossless: the bytes the lower channel saw equal the input exactly.
    REQUIRE(raw->m_sent == frame);
    // The OUT tap fired once with the verbatim frame at the first OUT sequence.
    REQUIRE(taps.size() == 1);
    REQUIRE(std::get<0>(taps[0]) == wire_direction::out);
    REQUIRE(std::get<1>(taps[0]) == 0u);
    REQUIRE(std::get<2>(taps[0]) == frame);
}

TEST_CASE("recording_channel re-emits inbound bytes verbatim and taps the IN frame", "[recording_channel][wire]")
{
    auto                         *raw = new test_lower;
    recording_channel<test_lower> ch{std::unique_ptr<test_lower>(raw)};

    std::vector<std::tuple<wire_direction, std::uint64_t, std::vector<std::byte>>> taps;
    ch.on_wire([&](wire_direction dir, std::uint64_t seq, std::span<const std::byte> b) { taps.emplace_back(dir, seq, std::vector<std::byte>(b.begin(), b.end())); });

    std::vector<std::byte> upward;
    ch.on_data([&](std::span<const std::byte> b) { upward.assign(b.begin(), b.end()); });

    const auto frame = blob(0xA0, 48);
    raw->feed(std::span<const std::byte>{frame});

    // The bytes re-emitted upward equal the inbound frame exactly (lossless).
    REQUIRE(upward == frame);
    // The IN tap fired once with the verbatim frame at the first IN sequence.
    REQUIRE(taps.size() == 1);
    REQUIRE(std::get<0>(taps[0]) == wire_direction::in);
    REQUIRE(std::get<1>(taps[0]) == 0u);
    REQUIRE(std::get<2>(taps[0]) == frame);
}

TEST_CASE("recording_channel with no tap installed never fires the edge", "[recording_channel][wire]")
{
    auto                         *raw = new test_lower;
    recording_channel<test_lower> ch{std::unique_ptr<test_lower>(raw)};

    std::vector<std::byte> upward;
    ch.on_data([&](std::span<const std::byte> b) { upward.assign(b.begin(), b.end()); });

    const auto frame = blob(0x01, 16);
    ch.send(std::span<const std::byte>{frame});
    raw->feed(std::span<const std::byte>{frame});

    // No tap and no recorder: bytes still flow losslessly, the edge is inert.
    REQUIRE(raw->m_sent == frame);
    REQUIRE(upward == frame);
}

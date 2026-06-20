#include "test_stream_inbound_common.h"

using namespace stream_inbound_fixture;

TEST_CASE("wire stream_inbound: bad magic raises invalid_magic once and cancels the timer",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        std::array<std::byte, header_size> garbage{};
        garbage.fill(std::byte{0xFF}); // first two bytes are not the magic
        f.feed(garbage);

        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::invalid_magic);

        // After a feed_error close, advancing the clock raises NO timeout.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) * 4);
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: an over-cap payload_len raises payload_too_large once and cancels "
          "the timer",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // A valid header whose claimed payload_len exceeds the reassembler cap.
        // encode_header preserves payload_len verbatim (encode_frame would overwrite
        // it with the actual payload size), so this drives the payload_too_large path.
        auto over = encode_header(make_header(k_max_reassembler_payload_bytes + 1));
        f.feed(over);

        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::payload_too_large);

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) * 4);
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: a span past the buffered-bytes cap raises buffer_overflow once and "
          "cancels the timer",
          "[wire][stream_inbound]")
{
    // The over-cap span is large; allocate it once and reuse it across iterations.
    const std::vector<std::byte> flood(k_max_reassembler_payload_bytes + header_size + 1,
                                       std::byte{0x00});

    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        f.feed(flood);

        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::buffer_overflow);

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) * 4);
        REQUIRE(f.c.closes == 1);
    }
}

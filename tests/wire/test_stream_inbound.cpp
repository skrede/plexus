#include "test_stream_inbound_common.h"

using namespace stream_inbound_fixture;

TEST_CASE("wire stream_inbound: a complete header with its payload withheld closes once on the "
          "floor deadline",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // Feed a COMPLETE header declaring a payload, then withhold every payload
        // byte. The reassembler decodes and erases the header, so buffered_bytes()
        // == 0 while it sits in reading_payload — the old keying saw "no buffered
        // bytes" and never armed. The size-proportional deadline keys off the
        // DECLARED length: 64 B / 1024 B/s = 62.5 ms clamps to the 500 ms floor.
        auto header = encode_header(make_header(64));
        f.feed(header);
        REQUIRE(f.c.frames == 0);
        REQUIRE(f.c.closes == 0);

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor));
        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);

        // A second advance with no further feed fires nothing more.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor));
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: a slow byte-dribble does not reset the deadline and closes once", "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // Dribble a partial header one byte at a time, advancing a little between
        // each, never completing the header. While the size is unknown the deadline
        // is the floor; a fresh frame arms it ONCE (idle->frame) and the continuing
        // dribble must NOT re-arm it. Cumulative advance crosses the floor -> close.
        auto frame      = encode_complete(64);
        const auto step = std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) / 8;

        for(std::size_t i = 0; i < header_size - 4; ++i)
        {
            f.feed(std::span<const std::byte>{frame}.subspan(i, 1));
            f.advance(step);
            // Each step is under the floor; if the dribble were re-arming, the
            // deadline would never elapse. It is not, so by the 8th+ step the
            // ORIGINAL arm fires.
        }

        REQUIRE(f.c.frames == 0);
        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor));
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: a sub-throughput large frame closes on the throughput floor", "[wire][stream_inbound]")
{
    constexpr int k_iterations = 50;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // A frame declaring N bytes whose payload arrives slower than min_throughput.
        // N = 4096 -> deadline = 4096/1024 s = 4 s. We feed the header (arming the
        // 4 s deadline), then deliver the payload in slices whose cumulative
        // virtual time exceeds 4 s before the last byte lands -> close fires.
        constexpr std::size_t k_payload = 4096;
        auto frame                      = encode_complete(k_payload);
        const auto deadline             = payload_deadline(k_payload);

        // Header arms the size-based deadline.
        f.feed(std::span<const std::byte>{frame}.subspan(0, header_size));
        REQUIRE(f.c.closes == 0);

        // Deliver the payload slower than the floor: advance past the deadline
        // across slices, NEVER completing in time. The dribble must not re-arm.
        std::size_t offset      = header_size;
        const std::size_t chunk = 256;
        const auto step         = deadline / 4; // 4 slices * step = deadline; we overrun it
        while(offset + chunk < frame.size() && f.c.closes == 0)
        {
            f.feed(std::span<const std::byte>{frame}.subspan(offset, chunk));
            offset += chunk;
            f.advance(step);
        }

        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);
        REQUIRE(f.c.frames == 0);
    }
}

TEST_CASE("wire stream_inbound: a frame that completes within its size-proportional deadline is "
          "never closed",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // N = 8192 -> deadline = 8 s, well above the 500 ms floor: this proves the
        // size-proportionality (a large frame gets a proportionally longer
        // allowance). Feed it in slices whose cumulative time stays UNDER 8 s and
        // complete it -> on_frame, zero closes.
        constexpr std::size_t k_payload = 8192;
        auto frame                      = encode_complete(k_payload);
        const auto deadline             = payload_deadline(k_payload);
        REQUIRE(deadline > std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor));

        const auto step         = deadline / 16; // many slices, each tiny
        std::size_t offset      = 0;
        const std::size_t chunk = 1024;
        while(offset + chunk < frame.size())
        {
            f.feed(std::span<const std::byte>{frame}.subspan(offset, chunk));
            offset += chunk;
            f.advance(step);
            REQUIRE(f.c.closes == 0);
        }
        f.feed(std::span<const std::byte>{frame}.subspan(offset));
        f.advance(step);

        REQUIRE(f.c.frames == 1);
        REQUIRE(f.c.closes == 0);

        // Frame done -> timer disarmed; a long advance fires nothing.
        f.advance(deadline * 4);
        REQUIRE(f.c.closes == 0);
    }
}

TEST_CASE("wire stream_inbound: a normal back-to-back complete-frame stream raises no close and "
          "disarms the timer",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        std::vector<std::byte> stream_bytes;
        constexpr int k_frames = 5;
        for(int n = 0; n < k_frames; ++n)
        {
            auto frame = encode_complete(32 + static_cast<std::size_t>(n) * 16);
            stream_bytes.insert(stream_bytes.end(), frame.begin(), frame.end());
        }
        f.feed(stream_bytes);

        REQUIRE(f.c.frames == k_frames);
        REQUIRE(f.c.closes == 0);

        // Every frame completed -> the timer is disarmed; a later advance fires
        // nothing.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) * 4);
        REQUIRE(f.c.closes == 0);
    }
}

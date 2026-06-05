// The deterministic stream_inbound detection oracle on the manual virtual clock:
// it proves the shared byte-stream framing-hardening component in isolation — no
// asio, no socket, no backend. A single inproc_executor<manual_clock> drives one
// inproc_timer<manual_clock>; the stream_inbound composes the production
// frame_reassembler by value and is fed bytes minted by the real codec path.
//
// The component keys a SIZE-PROPORTIONAL per-frame deadline off the in-progress
// frame's DECLARED payload length:
//   deadline = max(no_progress_floor, declared_payload_len / min_throughput).
// A frame that fails to COMPLETE within its deadline raises a single
// on_protocol_close(no_progress_timeout). The oracle proves, looped where
// behavioral:
//   - header-withheld: a complete header with its payload entirely withheld arms
//     the deadline; advancing past it closes ONCE (the case the old
//     buffered_bytes() keying missed — the reassembler erases the decoded header,
//     so buffered_bytes()==0 while it sits in reading_payload);
//   - slow dribble: a partial frame fed byte-by-byte, advancing a little between
//     each, never completing — the dribble does NOT keep resetting the deadline,
//     so it closes once the running deadline elapses;
//   - sub-throughput large frame: a frame whose payload arrives slower than
//     min_throughput closes on the throughput floor;
//   - legit completion: a frame whose bytes all arrive before its size-proportional
//     deadline delivers via on_frame with zero closes — including a large frame
//     whose proportionally longer deadline (N/min_throughput > floor) it meets,
//     proving the size-proportionality;
//   - normal stream: complete frames back-to-back deliver per frame, raise zero
//     closes, and disarm the timer between frames;
//   - each framing feed_error class (invalid_magic / payload_too_large /
//     buffer_overflow) raises on_protocol_close once with the matching close_cause
//     and cancels the timer, so a later advance fires no timeout.

#include "plexus/wire/stream_inbound.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_bus.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::wire::close_cause;
using plexus::wire::frame_header;
using plexus::wire::msg_type;
using plexus::wire::header_size;
using plexus::wire::stream_inbound;
using plexus::wire::stream_inbound_config;
using plexus::wire::complete_frame;
using plexus::wire::encode_frame;
using plexus::wire::encode_header;
using plexus::wire::k_max_reassembler_payload_bytes;

namespace {

// The virtual clock the no-progress timer fires from — lifted verbatim from the
// drop-seam oracle; a wire test needs only the clock + an inproc_executor and an
// inproc_timer over it, not the routing engine.
struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

using executor_t = inproc_executor<manual_clock>;
using timer_t = inproc_timer<manual_clock>;
using stream_t = stream_inbound<timer_t, executor_t &>;

// Test config: a 500 ms floor and a deliberately low 1024 B/s throughput floor so
// that payload_deadline(N) = N ms (N/1024 s) is directly legible — small frames
// (N below 512) clamp to the 500 ms floor, larger frames get a proportionally
// longer deadline. These are test tunables, not the production defaults.
constexpr auto k_floor = std::chrono::milliseconds(500);
constexpr std::size_t k_throughput = 1024;   // bytes/sec -> 1 ns per byte * 1e6

stream_inbound_config test_config()
{
    return stream_inbound_config{
            .no_progress_floor = std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor),
            .min_throughput_bytes_per_sec = k_throughput};
}

// payload_deadline(N) = N / throughput, in nanoseconds (mirrors the component).
std::chrono::nanoseconds payload_deadline(std::size_t n)
{
    return std::chrono::nanoseconds{static_cast<std::int64_t>(n) * 1'000'000'000
                                    / static_cast<std::int64_t>(k_throughput)};
}

frame_header make_header(std::uint64_t payload_len)
{
    return frame_header{.type = msg_type::unidirectional, .flags = 0, .session_id = 1,
                        .timestamp_ns = 0, .payload_len = payload_len};
}

// A complete frame minted by the production codec path.
std::vector<std::byte> encode_complete(std::size_t payload_size)
{
    std::vector<std::byte> payload(payload_size, std::byte{0xAB});
    return encode_frame(make_header(payload_size), payload);
}

struct counters
{
    int frames{0};
    int closes{0};
    std::optional<close_cause> last_cause;
};

// A stream_inbound + its executor/bus + the counting callbacks, so each case
// builds an isolated fixture on a freshly-reset clock.
struct fixture
{
    inproc_bus<manual_clock> bus;
    executor_t ex{bus};
    counters c;
    stream_t stream{ex, test_config()};

    fixture()
    {
        stream.on_frame([this](const complete_frame &) { ++c.frames; });
        stream.on_protocol_close([this](close_cause cause) { ++c.closes; c.last_cause = cause; });
    }

    void feed(std::span<const std::byte> bytes) { stream.feed(bytes); }
    void advance(std::chrono::nanoseconds d) { manual_clock::advance(d); ex.drain(); }
};

}

TEST_CASE("wire stream_inbound: a complete header with its payload withheld closes once on the floor deadline",
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

TEST_CASE("wire stream_inbound: a slow byte-dribble does not reset the deadline and closes once",
          "[wire][stream_inbound]")
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
        auto frame = encode_complete(64);
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

TEST_CASE("wire stream_inbound: a sub-throughput large frame closes on the throughput floor",
          "[wire][stream_inbound]")
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
        auto frame = encode_complete(k_payload);
        const auto deadline = payload_deadline(k_payload);

        // Header arms the size-based deadline.
        f.feed(std::span<const std::byte>{frame}.subspan(0, header_size));
        REQUIRE(f.c.closes == 0);

        // Deliver the payload slower than the floor: advance past the deadline
        // across slices, NEVER completing in time. The dribble must not re-arm.
        std::size_t offset = header_size;
        const std::size_t chunk = 256;
        const auto step = deadline / 4;   // 4 slices * step = deadline; we overrun it
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

TEST_CASE("wire stream_inbound: a frame that completes within its size-proportional deadline is never closed",
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
        auto frame = encode_complete(k_payload);
        const auto deadline = payload_deadline(k_payload);
        REQUIRE(deadline > std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor));

        const auto step = deadline / 16;   // many slices, each tiny
        std::size_t offset = 0;
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

TEST_CASE("wire stream_inbound: a normal back-to-back complete-frame stream raises no close and disarms the timer",
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

TEST_CASE("wire stream_inbound: bad magic raises invalid_magic once and cancels the timer",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        std::array<std::byte, header_size> garbage{};
        garbage.fill(std::byte{0xFF});   // first two bytes are not the magic
        f.feed(garbage);

        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::invalid_magic);

        // After a feed_error close, advancing the clock raises NO timeout.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor) * 4);
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: an over-cap payload_len raises payload_too_large once and cancels the timer",
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

TEST_CASE("wire stream_inbound: a span past the buffered-bytes cap raises buffer_overflow once and cancels the timer",
          "[wire][stream_inbound]")
{
    // The over-cap span is large; allocate it once and reuse it across iterations.
    const std::vector<std::byte> flood(k_max_reassembler_payload_bytes + header_size + 1, std::byte{0x00});

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

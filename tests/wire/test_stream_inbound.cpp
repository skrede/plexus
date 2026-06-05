// The deterministic stream_inbound detection oracle on the manual virtual clock:
// it proves the shared byte-stream framing-hardening component in isolation — no
// asio, no socket, no backend. A single inproc_executor<manual_clock> drives one
// inproc_timer<manual_clock>; the stream_inbound composes the production
// frame_reassembler by value and is fed bytes minted by the real codec path. It
// proves, looped where behavioral:
//   - slowloris: a valid header with its payload withheld arms the no-progress
//     timer; advancing the injected bound on the virtual clock raises
//     on_protocol_close ONCE with close_cause::no_progress_timeout, and a second
//     advance with no further feed fires nothing;
//   - each framing feed_error class (invalid_magic / payload_too_large /
//     buffer_overflow) raises on_protocol_close exactly once with the matching
//     close_cause and cancels the timer, so a later advance fires no timeout;
//   - progress-no-close: a large frame whose buffer grows across feeds (each feed
//     under the bound) completes before the bound elapses -> on_frame fires, zero
//     protocol-closes (the legitimate slow-large-frame case);
//   - normal stream: several complete frames back-to-back deliver per frame, raise
//     zero closes, and leave the buffer empty so the timer is disarmed.

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

constexpr auto k_bound = std::chrono::milliseconds(500);

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
    stream_t stream{ex, std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound)};

    fixture()
    {
        stream.on_frame([this](const complete_frame &) { ++c.frames; });
        stream.on_protocol_close([this](close_cause cause) { ++c.closes; c.last_cause = cause; });
    }

    void feed(std::span<const std::byte> bytes) { stream.feed(bytes); }
    void advance(std::chrono::nanoseconds d) { manual_clock::advance(d); ex.drain(); }
};

}

TEST_CASE("wire stream_inbound: a stalled partial frame arms the no-progress timer and closes once on timeout",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // Dribble a partial frame header (magic intact, the header incomplete) and
        // then stop: the reassembler keeps the fragment buffered, so buffered_bytes()
        // > 0 and the no-progress timer arms. (A byte-dribble slowloris is exactly an
        // incomplete frame that leaves bytes buffered; the reassembler exposes that as
        // buffered_bytes().)
        auto frame = encode_complete(64);
        f.feed(std::span<const std::byte>{frame}.subspan(0, header_size - 4));
        REQUIRE(f.c.frames == 0);
        REQUIRE(f.c.closes == 0);

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound));
        REQUIRE(f.c.closes == 1);
        REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);

        // A second advance with no further feed fires nothing more.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound));
        REQUIRE(f.c.closes == 1);
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
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound) * 4);
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

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound) * 4);
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

        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound) * 4);
        REQUIRE(f.c.closes == 1);
    }
}

TEST_CASE("wire stream_inbound: a slow-but-advancing large frame is never falsely closed",
          "[wire][stream_inbound]")
{
    constexpr int k_iterations = 100;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        fixture f;

        // A large frame fed in growing slices, each advance under the bound. Because
        // the buffer grows on every feed the timer re-arms fresh, so the cumulative
        // wall-time exceeds the bound yet no false close fires.
        constexpr std::size_t k_payload = 4096;
        auto frame = encode_complete(k_payload);
        const auto step = std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound) / 2;

        std::size_t offset = 0;
        const std::size_t chunk = 512;
        while(offset + chunk < frame.size())
        {
            f.feed(std::span<const std::byte>{frame}.subspan(offset, chunk));
            offset += chunk;
            f.advance(step);   // under the bound; the grown buffer re-armed the timer
            REQUIRE(f.c.closes == 0);
        }
        // Feed the remainder: the frame completes before the last arm elapses.
        f.feed(std::span<const std::byte>{frame}.subspan(offset));
        f.advance(step);

        REQUIRE(f.c.frames == 1);
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

        // The buffer is empty -> the timer is disarmed; a later advance fires nothing.
        f.advance(std::chrono::duration_cast<std::chrono::nanoseconds>(k_bound) * 4);
        REQUIRE(f.c.closes == 0);
    }
}

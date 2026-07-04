#ifndef HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_STREAM_INBOUND_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_STREAM_INBOUND_COMMON_H

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
// on_protocol_close(no_progress_timeout).

#include "plexus/stream/stream_inbound.h"
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
using plexus::stream::stream_inbound;
using plexus::stream::stream_inbound_config;
using plexus::wire::complete_frame;
using plexus::wire::encode_frame;
using plexus::wire::encode_header;
using plexus::wire::k_max_reassembler_payload_bytes;

namespace stream_inbound_fixture {

// The virtual clock the no-progress timer fires from — lifted verbatim from the
// drop-seam oracle; a wire test needs only the clock + an inproc_executor and an
// inproc_timer over it, not the routing engine.
struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    [[maybe_unused]] static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

using executor_t = inproc_executor<manual_clock>;
using timer_t    = inproc_timer<manual_clock>;
using stream_t   = stream_inbound<timer_t, executor_t &>;

// Test config: a 500 ms floor and a deliberately low 1024 B/s throughput floor so
// that payload_deadline(N) = N ms (N/1024 s) is directly legible — small frames
// (N below 512) clamp to the 500 ms floor, larger frames get a proportionally
// longer deadline. These are test tunables, not the production defaults.
constexpr auto k_floor             = std::chrono::milliseconds(500);
constexpr std::size_t k_throughput = 1024; // bytes/sec -> 1 ns per byte * 1e6

inline stream_inbound_config test_config()
{
    return stream_inbound_config{.no_progress_floor = std::chrono::duration_cast<std::chrono::nanoseconds>(k_floor), .min_throughput_bytes_per_sec = k_throughput};
}

// payload_deadline(N) = N / throughput, in nanoseconds (mirrors the component).
inline std::chrono::nanoseconds payload_deadline(std::size_t n)
{
    return std::chrono::nanoseconds{static_cast<std::int64_t>(n) * 1'000'000'000 / static_cast<std::int64_t>(k_throughput)};
}

inline frame_header make_header(std::uint64_t payload_len)
{
    return frame_header{.type = msg_type::unidirectional, .flags = 0, .session_id = 1, .timestamp_ns = 0, .payload_len = payload_len};
}

// A complete frame minted by the production codec path.
inline std::vector<std::byte> encode_complete(std::size_t payload_size)
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
        stream.on_protocol_close(
                [this](close_cause cause)
                {
                    ++c.closes;
                    c.last_cause = cause;
                });
    }

    void feed(std::span<const std::byte> bytes)
    {
        stream.feed(bytes);
    }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }
};

}

#endif

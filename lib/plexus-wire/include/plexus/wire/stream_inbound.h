#ifndef HPP_GUARD_PLEXUS_WIRE_STREAM_INBOUND_H
#define HPP_GUARD_PLEXUS_WIRE_STREAM_INBOUND_H

#include "plexus/wire/frame_reassembler.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <system_error>

namespace plexus::wire {

// Why a dedicated cause enum is raised on close, rather than feed_error: a close
// signal carries only actionable reasons — the three real framing violations plus
// the no-progress timeout — never feed_error::none (not a cause) nor the dead
// no_progress enumerator (the timer keys off a size-proportional per-frame
// deadline, not a reassembler signal).
enum class close_cause : std::uint8_t
{
    invalid_magic,
    payload_too_large,
    buffer_overflow,
    no_progress_timeout
};

inline close_cause to_close_cause(feed_error error)
{
    switch(error)
    {
        case feed_error::payload_too_large: return close_cause::payload_too_large;
        case feed_error::buffer_overflow:   return close_cause::buffer_overflow;
        default:                            return close_cause::invalid_magic;
    }
}

// Tunables for the per-frame no-progress deadline. Both are required-with-default:
// a node may override them, but the defaults are safe and generous.
//   no_progress_floor    — the minimum deadline any in-progress frame is granted.
//                          A generous small-frame/partial-header allowance so a
//                          legitimate slow handshake or tiny frame never trips.
//   min_throughput_bytes_per_sec — the average-throughput floor a frame's payload
//                          must clear. 64 KiB/s sits below any real broadband/WAN/
//                          LAN link, so legitimate transfers pass, while bounding
//                          the worst-case hold for the 16 MiB payload cap to ~256 s
//                          (cap / min_throughput). A value of 0 disables the
//                          throughput floor — the deadline collapses to the floor.
struct stream_inbound_config
{
    std::chrono::nanoseconds no_progress_floor{std::chrono::seconds{30}};
    std::size_t min_throughput_bytes_per_sec{64 * 1024};
    std::size_t max_payload_size{k_max_reassembler_payload_bytes};
    std::size_t buffered_bytes_cap{k_max_reassembler_payload_bytes + header_size};
};

// The shared byte-stream framing-hardening detection layer. It composes the
// frame_reassembler by value, consumes the feed_error the channel discards today,
// and owns a no-progress (slowloris) timer keyed off a SIZE-PROPORTIONAL per-frame
// deadline: deadline = max(no_progress_floor, declared_payload_len / min_throughput).
// A frame that fails to COMPLETE within its deadline raises a single
// on_protocol_close(no_progress_timeout). This one timer simultaneously enforces an
// absolute ceiling (bounded by payload_cap / min_throughput) and an average-
// throughput floor, catching header-withheld, slow-dribble, total-stall, and sub-
// floor transfers while never closing a frame that completes in time. Parametrized
// on <Timer, Executor> — the two Policy-supplied types it actually uses — to stay
// free of any plexus::Policy dependency and to break the channel<->policy include
// cycle. It owns no logger, no RNG, no socket; it only detects and signals.
template <typename Timer, typename Executor>
class stream_inbound
{
public:
    stream_inbound(Executor ex, stream_inbound_config cfg)
        : m_reassembler(cfg.max_payload_size, cfg.buffered_bytes_cap)
        , m_timer(ex)
        , m_cfg(cfg)
    {
    }

    void on_frame(plexus::detail::move_only_function<void(const complete_frame &)> cb) { m_on_frame = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    void feed(std::span<const std::byte> bytes)
    {
        const bool was_in_progress = m_reassembler.frame_in_progress();

        auto result = m_reassembler.feed(bytes);

        for(const auto &frame : result.frames)
            if(m_on_frame)
                m_on_frame(frame);

        if(result.error != feed_error::none)
        {
            m_timer.cancel();
            if(m_on_protocol_close)
                m_on_protocol_close(to_close_cause(result.error));
            return;
        }

        reframe_timer(was_in_progress, !result.frames.empty());
    }

    void shutdown() { m_timer.cancel(); }

private:
    // Drive the per-frame deadline across this feed's transitions. Idle between
    // frames -> disarm. Otherwise compute the in-progress frame's size-proportional
    // deadline and re-arm it ONLY on a real transition: a frame just started
    // (idle->frame, or one completed and another is pending), or the header just
    // became known so the deadline grew from the floor to its size-based value.
    // A frame continuously mid-assembly at the SAME deadline is left untouched so
    // the running deadline expires — this is what catches dribble and header-
    // withhold (the cases re-arm-on-growth used to reset forever).
    void reframe_timer(bool was_in_progress, bool completed)
    {
        if(!m_reassembler.frame_in_progress())
        {
            m_timer.cancel();
            m_armed_deadline = std::chrono::nanoseconds{0};
            return;
        }

        const auto want = deadline_for_current_frame();
        if(!was_in_progress || completed || want != m_armed_deadline)
            arm(want);
    }

    std::chrono::nanoseconds deadline_for_current_frame() const
    {
        auto plen = m_reassembler.pending_payload_len();
        if(!plen)
            return m_cfg.no_progress_floor;
        return std::max(m_cfg.no_progress_floor, payload_deadline(*plen));
    }

    // N / min_throughput as a nanosecond duration. min_throughput == 0 disables
    // the floor (deadline collapses to no_progress_floor). N <= 16 MiB and the
    // 1e9 scale fit comfortably in int64, so the multiply cannot overflow.
    std::chrono::nanoseconds payload_deadline(std::size_t n) const
    {
        if(m_cfg.min_throughput_bytes_per_sec == 0)
            return std::chrono::nanoseconds{0};
        const std::int64_t ns =
            static_cast<std::int64_t>(n) * 1'000'000'000
            / static_cast<std::int64_t>(m_cfg.min_throughput_bytes_per_sec);
        return std::chrono::nanoseconds{ns};
    }

    void arm(std::chrono::nanoseconds d)
    {
        m_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(d));
        m_timer.async_wait([this](std::error_code ec) {
            if(ec)
                return;   // cancelled by a new frame, completion, or shutdown
            if(m_on_protocol_close)
                m_on_protocol_close(close_cause::no_progress_timeout);
        });
        m_armed_deadline = d;
    }

    frame_reassembler m_reassembler;
    Timer m_timer;
    stream_inbound_config m_cfg;
    std::chrono::nanoseconds m_armed_deadline{0};
    plexus::detail::move_only_function<void(const complete_frame &)> m_on_frame;
    plexus::detail::move_only_function<void(close_cause)> m_on_protocol_close;
};

}

#endif

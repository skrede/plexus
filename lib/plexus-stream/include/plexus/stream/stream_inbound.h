#ifndef HPP_GUARD_PLEXUS_STREAM_STREAM_INBOUND_H
#define HPP_GUARD_PLEXUS_STREAM_STREAM_INBOUND_H

#include "plexus/detail/compat.h"

#include "plexus/io/detail/pending_ops.h"

#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <system_error>

namespace plexus::stream {

struct stream_inbound_config
{
    std::chrono::nanoseconds no_progress_floor{std::chrono::seconds{30}};
    // 64 KiB/s sits below any real broadband/WAN/LAN link, so legitimate transfers pass while the
    // worst-case hold for the payload cap is bounded to cap / min_throughput. 0 disables the floor.
    std::size_t min_throughput_bytes_per_sec{64 * 1024};
    std::size_t max_payload_size{wire::k_max_reassembler_payload_bytes};
    std::size_t buffered_bytes_cap{wire::k_max_reassembler_payload_bytes + wire::header_size};
};

inline stream_inbound_config with_message_limits(stream_inbound_config cfg, std::size_t max_payload_size, std::size_t reassembly_budget) noexcept
{
    cfg.max_payload_size   = max_payload_size;
    cfg.buffered_bytes_cap = std::max(reassembly_budget, max_payload_size + wire::header_size);
    return cfg;
}

// A byte-stream framing-hardening layer over wire::frame_reassembler. It owns a no-progress
// (slowloris) timer keyed off a size-proportional per-frame deadline:
// deadline = max(no_progress_floor, declared_payload_len / min_throughput). A frame that fails to
// complete within its deadline raises a single on_protocol_close(no_progress_timeout), catching
// header-withheld, slow-dribble, total-stall, and sub-floor transfers. Its outstanding wait is
// counted in pending(), which shutdown() closes, so an owner destroys it only once that settled.
template<typename Timer, typename Executor>
class stream_inbound
{
public:
    stream_inbound(Executor ex, stream_inbound_config cfg)
            : m_timer(ex)
            , m_shut_down(false)
            , m_cfg(cfg)
            , m_timer_generation(0)
            , m_pending()
            , m_reassembler(cfg.max_payload_size, cfg.buffered_bytes_cap)
            , m_armed_deadline(0)
            , m_protocol_close_cb()
            , m_frame_cb()
    {
    }

    void on_frame(plexus::detail::move_only_function<void(const wire::complete_frame &)> cb)
    {
        m_frame_cb = std::move(cb);
    }

    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_protocol_close_cb = std::move(cb);
    }

    void feed(std::span<const std::byte> bytes)
    {
        const bool was_in_progress = m_reassembler.frame_in_progress();

        auto result = m_reassembler.feed(bytes);

        for(const auto &frame : result.frames)
            if(m_frame_cb)
                m_frame_cb(frame);

        if(result.error != wire::feed_error::none)
        {
            cancel_timer();
            if(m_protocol_close_cb)
                m_protocol_close_cb(wire::to_close_cause(result.error));
            return;
        }

        reframe_timer(was_in_progress, !result.frames.empty());
    }

    void shutdown()
    {
        m_shut_down = true;
        cancel_timer();
        m_pending.close();
    }

    io::detail::pending_ops &pending() noexcept
    {
        return m_pending;
    }

private:
    Timer m_timer;
    bool m_shut_down;
    stream_inbound_config m_cfg;
    std::uint64_t m_timer_generation;
    io::detail::pending_ops m_pending;
    wire::frame_reassembler m_reassembler;
    std::chrono::nanoseconds m_armed_deadline;
    plexus::detail::move_only_function<void(wire::close_cause)> m_protocol_close_cb;
    plexus::detail::move_only_function<void(const wire::complete_frame &)> m_frame_cb;

    // Re-arm the per-frame deadline only on a real transition (a frame just started, or the header
    // just became known so the deadline grew from the floor); a frame continuously mid-assembly at
    // the same deadline is left untouched so the running deadline expires — this catches dribble.
    void reframe_timer(bool was_in_progress, bool completed)
    {
        if(m_shut_down)
            return;
        if(!m_reassembler.frame_in_progress())
        {
            cancel_timer();
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

    // N / min_throughput as a nanosecond duration (min_throughput == 0 disables the floor). N and
    // the 1e9 scale fit comfortably in int64, so the multiply cannot overflow.
    std::chrono::nanoseconds payload_deadline(std::size_t n) const
    {
        if(m_cfg.min_throughput_bytes_per_sec == 0)
            return std::chrono::nanoseconds{0};
        const std::int64_t ns = static_cast<std::int64_t>(n) * 1'000'000'000 / static_cast<std::int64_t>(m_cfg.min_throughput_bytes_per_sec);
        return std::chrono::nanoseconds{ns};
    }

    void arm(std::chrono::nanoseconds d)
    {
        const std::uint64_t gen = ++m_timer_generation;
        m_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(d));
        m_pending.begin();
        m_timer.async_wait([this, gen](std::error_code ec) { on_expiry(gen, ec); });
        m_armed_deadline = d;
    }

    // The count ends first, on every completion, so pending() stays exact whatever the outcome; only
    // then does the object's own state decide whether the expiry acts.
    void on_expiry(std::uint64_t gen, std::error_code ec)
    {
        m_pending.end();
        if(ec || m_shut_down)
            return;
        // An already-expired timer's completion is queued with ec==success and cannot be
        // recalled by cancel(); a generation bumped by any re-arm or cancel since this wait
        // armed marks the completion stale, so it must not close a channel that moved on.
        if(gen != m_timer_generation || !m_reassembler.frame_in_progress())
            return;
        if(m_protocol_close_cb)
            m_protocol_close_cb(wire::close_cause::no_progress_timeout);
    }

    // Bumping the generation defeats a stale queued expiry even across an equal-deadline re-arm,
    // which m_timer.cancel() alone cannot do (it cannot recall an already-expired completion).
    void cancel_timer()
    {
        ++m_timer_generation;
        m_timer.cancel();
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_WIRE_STREAM_INBOUND_H
#define HPP_GUARD_PLEXUS_WIRE_STREAM_INBOUND_H

#include "plexus/wire/frame_reassembler.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::wire {

// Why a dedicated cause enum is raised on close, rather than feed_error: a close
// signal carries only actionable reasons — the three real framing violations plus
// the no-progress timeout — never feed_error::none (not a cause) nor the dead
// no_progress enumerator (the timer keys off buffered_bytes(), not a reassembler
// signal).
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

// The shared byte-stream framing-hardening detection layer. It composes the
// frame_reassembler by value, consumes the feed_error the channel discards today,
// and owns a no-progress (slowloris) timer keyed off buffered_bytes(): a partial
// frame that stops advancing raises a single on_protocol_close after the injected
// bound. Parametrized on <Timer, Executor> — the two Policy-supplied types it
// actually uses — to stay free of any plexus::Policy dependency and to break the
// channel<->policy include cycle. It owns no logger, no RNG, no socket; it only
// detects and signals.
template <typename Timer, typename Executor>
class stream_inbound
{
public:
    stream_inbound(Executor ex, std::chrono::nanoseconds no_progress_bound)
        : m_timer(ex)
        , m_bound(no_progress_bound)
    {
    }

    void on_frame(plexus::detail::move_only_function<void(const complete_frame &)> cb) { m_on_frame = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    void feed(std::span<const std::byte> bytes)
    {
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

        update_timer(result.frames.empty());
    }

    void shutdown() { m_timer.cancel(); }

private:
    // Arm/disarm the no-progress timer off the reassembler buffer. Empty buffer ->
    // disarm. A grown buffer or a frame completed this feed is progress -> re-arm
    // fresh (this is the legitimate slow-large-frame fix). An unchanged non-empty
    // buffer is a stalled partial -> leave the running deadline untouched.
    void update_timer(bool no_frame_completed)
    {
        const std::size_t buffered = m_reassembler.buffered_bytes();
        if(buffered == 0)
        {
            m_timer.cancel();
            m_last_buffered = 0;
            return;
        }
        if(buffered > m_last_buffered || !no_frame_completed)
            arm();
        m_last_buffered = buffered;
    }

    void arm()
    {
        m_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(m_bound));
        m_timer.async_wait([this](std::error_code ec) {
            if(ec)
                return;   // cancelled by progress, completion, or shutdown
            if(m_on_protocol_close)
                m_on_protocol_close(close_cause::no_progress_timeout);
        });
    }

    frame_reassembler m_reassembler;
    Timer m_timer;
    std::chrono::nanoseconds m_bound;
    std::size_t m_last_buffered{0};
    plexus::detail::move_only_function<void(const complete_frame &)> m_on_frame;
    plexus::detail::move_only_function<void(close_cause)> m_on_protocol_close;
};

}

#endif

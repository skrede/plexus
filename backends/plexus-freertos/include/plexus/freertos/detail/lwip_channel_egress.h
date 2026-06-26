#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_EGRESS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_EGRESS_H

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/stream/stream_socket.h"
#include "plexus/stream/detail/send_queue.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>

namespace plexus::freertos::detail {

// The egress/drain half of an lwip_channel: a bounded send_queue over a borrowed connected
// stream_socket. The borrowed socket and the on_error hook outlive this helper (the owning channel
// holds both). lwIP has no scatter writev, so the gathered views are sent one by one.
//
// Two distinct failure classes ride this path (research §5), kept textually separate so a reader
// sees soft != hard:
//   * SOFT/transient — a full send_queue (egress cap exceeded) or a short/zero send the socket
//     folded from EWOULDBLOCK/EAGAIN/ERR_MEM. This is LOCAL congestion: the congestion QoS absorbs
//     it (block backpressures the producer with would_block; drop_newest sheds+counts). It NEVER
//     fires the connection-drop on_error and NEVER tears the channel down.
//   * HARD/connection — ECONNRESET/EPIPE, surfaced by the socket's closed predicate after the
//     gather. This fires on_error(connection_reset) so the engine re-dials.
template<plexus::stream::stream_socket S>
class lwip_channel_egress
{
public:
    using error_cb   = plexus::detail::move_only_function<void(plexus::io::io_error)>;
    using buffer_seq = plexus::stream::detail::send_queue::buffer_sequence;
    using completion = plexus::stream::detail::send_queue::completion;

    lwip_channel_egress(S &socket, error_cb &on_error, std::size_t egress_cap, plexus::io::congestion congestion = plexus::io::congestion::block)
            : m_socket(socket)
            , m_on_error(on_error)
            , m_congestion(congestion)
            , m_send_queue([this](buffer_seq views, completion done) { drain_views(views, std::move(done)); }, egress_cap)
    {
    }

    // A frame the bounded queue refuses (egress cap exceeded) is local congestion, not a drop: under
    // drop_newest shed it and count; under block backpressure the producer via would_block. The
    // channel stays open either way — this never re-dials.
    void send(std::span<const std::byte> framed)
    {
        if(m_send_queue.enqueue(framed))
            return;
        if(m_congestion == plexus::io::congestion::drop_newest)
        {
            ++m_dropped;
            return;
        }
        if(m_on_error)
            m_on_error(plexus::io::io_error::would_block);
    }

    // A soft short/zero send leaves the gather resident (no done() — see drain_views); the send_queue
    // won't re-issue it on its own (its drive() chains only off a completion), so the channel's poll
    // loop pumps the stalled gather here until it flushes.
    void poll_egress()
    {
        if(m_pending)
            drain_views(m_views, std::move(m_done));
    }

    void close()
    {
        m_pending = false;
        m_send_queue.close_and_drain();
    }

    std::size_t dropped() const noexcept
    {
        return m_dropped;
    }

private:
    // Send the gather, resuming past the bytes a prior turn already wrote (m_sent). The send_queue
    // re-issues the SAME front-N nodes per turn and its completion (send_queue.h:142-152) IGNORES the
    // bool and pops them on ANY call, so a soft short/zero send MUST NOT call done() — that would pop
    // and discard the unsent tail (data loss / success-of-zero-bytes). On a soft stall stash the
    // gather + completion and return; poll_egress() retries from m_sent. A hard close fires on_error.
    // Only a fully-flushed gather resets the cursor and calls done(true).
    void drain_views(buffer_seq views, completion done)
    {
        std::size_t skip = m_sent;
        for(const auto &v : views)
        {
            if(skip >= v.size())
            {
                skip -= v.size();
                continue;
            }
            const auto chunk        = v.subspan(skip);
            const std::size_t wrote = m_socket.send(chunk);
            m_sent += wrote;
            skip = 0;
            if(wrote < chunk.size())
                return stall(views, std::move(done)); // a view short-sent: retry from m_sent
        }
        m_pending = false;
        m_sent    = 0;
        done(true);
    }

    // A hard close (the socket's closed predicate) is the distinct connection-drop seam: fire on_error
    // ONCE and abandon the gather (a dead connection re-dials, it does not retry). A soft stall keeps
    // the gather resident and stashes its completion so poll_egress() retries it next turn.
    void stall(buffer_seq views, completion done)
    {
        if(m_socket.closed())
        {
            if(m_on_error)
                m_on_error(plexus::io::io_error::connection_reset);
            m_pending = false;
            return;
        }
        m_views   = views;
        m_done    = std::move(done);
        m_pending = true;
    }

    S          &m_socket;
    error_cb   &m_on_error;
    buffer_seq  m_views{};
    completion  m_done{};
    plexus::io::congestion             m_congestion;
    std::size_t                        m_dropped{0};
    std::size_t                        m_sent{0};
    bool                               m_pending{false};
    plexus::stream::detail::send_queue m_send_queue;
};

}

#endif

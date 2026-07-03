#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_IO_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/wire/frame.h"

#include "plexus/wire_bytes.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/stream/detail/send_queue.h"

#include <asio/post.hpp>
#include <asio/write.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio::detail {

// Heap-held teardown-quiescence state shared by a channel and every async op it issues. asio
// aborts only PENDING ops, so a completion already queued when the owner destroys the channel
// still runs; a completion captures a copy of this block, so the reads it makes at entry hit
// owner-stable storage, not the freed channel. `alive` is cleared by ~stream_channel before the
// socket teardown that aborts the ops, so a late completion no-ops; `outstanding` counts
// issued-but-not-completed reads/writes; `closed_fired` makes the posted on_closed fire exactly
// once across the close() and stream_fail edges.
struct channel_liveness
{
    std::size_t outstanding = 0;
    bool alive              = true;
    bool closed_fired       = false;
};

template<typename Ch>
void stream_on_write_queue_full(Ch &c)
{
    if(c.m_congestion == io::congestion::drop_newest)
    {
        ++c.m_dropped;
        return;
    }
    if(c.m_on_error_cb)
        c.m_on_error_cb(io::io_error::would_block);
}

template<typename Ch>
void stream_fail(Ch &c, const std::error_code &ec)
{
    if(ec == ::asio::error::operation_aborted || !c.m_open)
        return;
    c.m_open = false;
    c.m_egress.close(); // stop the drain so the failed write does not chain
    c.m_inbound.shutdown();
    c.shutdown_socket(); // a stalled/failed write must not survive into a redial
    auto mapped = detail::map_error(ec);
    if(c.m_on_error_cb)
        c.m_on_error_cb(mapped);
    c.post_on_closed(); // posted, exactly once — the same discipline as close()
}

// A wire protocol violation: fire the protocol-close seam, then close() (on_closed only) — NEVER
// fail()/on_error.
template<typename Ch>
void stream_handle_protocol_close(Ch &c, wire::close_cause cause)
{
    if(c.m_on_protocol_close_cb)
        c.m_on_protocol_close_cb(cause);
    c.close();
}

// The posted closure captures the owner, keeping the bytes alive past this call (the post runs
// later).
template<typename Ch>
void stream_post_frame(Ch &c, const wire::complete_frame &frame)
{
    wire_bytes<> owned{frame.payload};
    ::asio::post(c.m_io,
                 [&c, owned = std::move(owned)]
                 {
                     if(c.m_on_data_cb)
                         c.m_on_data_cb(static_cast<std::span<const std::byte>>(owned));
                 });
}

template<typename Ch>
void stream_wire_inbound(Ch &c)
{
    c.m_inbound.on_frame([&c](const wire::complete_frame &f) { stream_post_frame(c, f); });
    c.m_inbound.on_protocol_close([&c](wire::close_cause cc) { stream_handle_protocol_close(c, cc); });
}

template<typename Ch>
void stream_do_read(Ch &c)
{
    ++c.m_life->outstanding;
    c.m_stream.async_read_some(::asio::buffer(c.m_read_buf),
                               [&c, life = c.m_life](std::error_code ec, std::size_t n)
                               {
                                   --life->outstanding;
                                   if(!life->alive) // the channel was destroyed with this read queued
                                       return;
                                   if(ec)
                                       return stream_fail(c, ec);
                                   c.m_inbound.feed(std::span<const std::byte>{c.m_read_buf.data(), n});
                                   if(c.m_open) // a protocol-close may have torn the socket down
                                       stream_do_read(c);
                               });
}

// Gather the block-owned node views into one ConstBufferSequence and issue a SINGLE async_write
// (asio lowers it to one writev/WSASend).
template<typename Ch>
stream::detail::send_queue::send_sink stream_make_send_sink(Ch &c)
{
    return [&c](stream::detail::send_queue::buffer_sequence views, stream::detail::send_queue::completion done)
    {
        c.m_gather.clear();
        c.m_gather.reserve(views.size());
        for(const auto &v : views)
            c.m_gather.emplace_back(v.data(), v.size());
        ++c.m_life->outstanding;
        ::asio::async_write(c.m_stream, c.m_gather,
                            [&c, life = c.m_life, done = std::move(done)](std::error_code ec, std::size_t) mutable
                            {
                                --life->outstanding;
                                // The channel (and its m_egress) may already be freed; run the
                                // send_queue completion — which reads m_egress.m_open — only while
                                // the owner is alive, so that read is never against freed storage.
                                if(!life->alive)
                                    return;
                                if(ec)
                                    stream_fail(c, ec);
                                done(!ec);
                            });
    };
}

}

#endif

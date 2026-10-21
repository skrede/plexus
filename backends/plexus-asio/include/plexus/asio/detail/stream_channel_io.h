#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_STREAM_CHANNEL_IO_H

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/stream_channel_lifecycle.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"

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
    stream_close_with(c,
                      [&c, mapped = detail::map_error(ec)]
                      {
                          auto on_error = std::exchange(c.m_on_error_cb, nullptr);
                          if(on_error)
                              on_error(mapped);
                          c.m_egress.close();
                      });
}

// A wire protocol violation: fire the protocol-close seam, then close() (on_closed only) — NEVER
// fail()/on_error.
template<typename Ch>
void stream_handle_protocol_close(Ch &c, wire::close_cause cause)
{
    auto on_protocol_close = std::exchange(c.m_on_protocol_close_cb, nullptr);
    if(on_protocol_close)
        on_protocol_close(cause);
    c.close();
}

template<typename Ch>
void stream_post_frame(Ch &c, const wire::complete_frame &frame)
{
    if(c.m_ops.closing())
        return;
    c.m_ops.begin();
    ::asio::post(c.m_io,
                 [&c, owned = wire_bytes<>{frame.payload}]
                 {
                     c.m_ops.end();
                     if(c.m_on_data_cb)
                         c.m_on_data_cb(static_cast<std::span<const std::byte>>(owned));
                 });
}

template<typename Ch>
void stream_wire_hooks(Ch &c)
{
    c.m_inbound.on_frame([&c](const wire::complete_frame &f) { stream_post_frame(c, f); });
    c.m_inbound.on_protocol_close([&c](wire::close_cause cc) { stream_handle_protocol_close(c, cc); });
    c.m_ops.on_settled([&c] { stream_notify_closed(c); });
}

template<typename Ch>
void stream_do_read(Ch &c)
{
    c.m_ops.begin();
    c.m_stream.async_read_some(::asio::buffer(c.m_read_buf),
                               [&c](std::error_code ec, std::size_t n)
                               {
                                   c.m_ops.end();
                                   if(ec)
                                       return stream_fail(c, ec);
                                   if(!c.m_open)
                                       return;
                                   c.m_inbound.feed(std::span<const std::byte>{c.m_read_buf.data(), n});
                                   // A protocol close raised by the feed closes the channel.
                                   if(c.m_open)
                                       stream_do_read(c);
                               });
}

// Gather the queue-owned node views into one ConstBufferSequence and issue a SINGLE async_write
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
        c.m_ops.begin();
        ::asio::async_write(c.m_stream, c.m_gather,
                            [&c, done = std::move(done)](std::error_code ec, std::size_t) mutable
                            {
                                c.m_ops.end();
                                if(ec)
                                    stream_fail(c, ec);
                                done(!ec);
                            });
    };
}

}

#endif

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

// The send-queue drive + read-loop glue for stream_channel, relocated by friendship: each helper
// reaches the stream/inbound/egress/sink members through the channel reference. Names carry a
// stream_ prefix where a bare name would collide with another channel's detail glue.

// The per-connection congestion safety net for the direct-send bypass paths. drop_newest sheds
// the frame at the publisher (counted); block surfaces would_block (the stall edge). Either way
// the call returns without blocking.
template<typename Ch>
void stream_on_write_queue_full(Ch &c)
{
    if(c.m_congestion == io::congestion::drop_newest)
    {
        ++c.m_dropped;
        return;
    }
    if(c.m_on_error)
        c.m_on_error(io::io_error::would_block);
}

template<typename Ch>
void stream_fail(Ch &c, const std::error_code &ec)
{
    if(ec == ::asio::error::operation_aborted || !c.m_open)
        return;
    c.m_open = false;
    c.m_egress.close(); // stop the serial drain so the failed write does not chain
    c.m_inbound.shutdown();
    auto mapped = detail::map_error(ec);
    if(c.m_on_error)
        c.m_on_error(mapped);
    if(c.m_on_closed)
        c.m_on_closed();
}

// A peer that misbehaved on the wire: fire the distinct protocol-close seam, then tear down via
// the on_closed-only close() path — NEVER fail()/on_error.
template<typename Ch>
void stream_handle_protocol_close(Ch &c, wire::close_cause cause)
{
    if(c.m_on_protocol_close)
        c.m_on_protocol_close(cause);
    c.close();
}

// The stream_inbound on_frame target: the reassembler already materialized the full header-on
// frame as the owning shared_bytes, so on_data delivers it verbatim. The posted closure CAPTURES
// the owner, keeping the bytes alive past this call (the post runs later).
template<typename Ch>
void stream_post_frame(Ch &c, const wire::complete_frame &frame)
{
    wire_bytes<> owned{frame.payload};
    ::asio::post(c.m_io,
                 [&c, owned = std::move(owned)]
                 {
                     if(c.m_on_data)
                         c.m_on_data(static_cast<std::span<const std::byte>>(owned));
                 });
}

template<typename Ch>
void stream_wire_inbound(Ch &c)
{
    c.m_inbound.on_frame([&c](const wire::complete_frame &f) { stream_post_frame(c, f); });
    c.m_inbound.on_protocol_close([&c](wire::close_cause cc)
                                  { stream_handle_protocol_close(c, cc); });
}

template<typename Ch>
void stream_do_read(Ch &c)
{
    c.m_stream.async_read_some(::asio::buffer(c.m_read_buf),
                               [&c](std::error_code ec, std::size_t n)
                               {
                                   if(ec)
                                       return stream_fail(c, ec);
                                   c.m_inbound.feed(
                                           std::span<const std::byte>{c.m_read_buf.data(), n});
                                   if(c.m_open) // a protocol-close may have torn the socket down
                                       stream_do_read(c);
                               });
}

// The irreducible asio send-sink the stream send_queue block drives: gather the block-owned node
// views into one ConstBufferSequence and issue a SINGLE async_write (asio lowers it to one
// writev/WSASend; for TLS, OpenSSL coalesces the plaintext into fewer records). On a socket error
// the channel fails (which closes the block), so the completion's open-guard stops the chain.
template<typename Ch>
stream::detail::send_queue::send_sink stream_make_send_sink(Ch &c)
{
    return [&c](stream::detail::send_queue::buffer_sequence views,
                stream::detail::send_queue::completion      done)
    {
        c.m_gather.clear();
        c.m_gather.reserve(views.size());
        for(const auto &v : views)
            c.m_gather.emplace_back(v.data(), v.size());
        ::asio::async_write(c.m_stream, c.m_gather,
                            [&c, done = std::move(done)](std::error_code ec, std::size_t) mutable
                            {
                                if(ec)
                                    stream_fail(c, ec);
                                done(!ec);
                            });
    };
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H

#include "plexus/wire/close_cause.h"

#include <asio/post.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <system_error>

namespace plexus::asio::detail {

// Post an owning copy so the bytes survive the deferred delivery — the read buffer is reused
// on the next read.
template<typename Ch>
void serial_post_frame(Ch &c, std::span<const std::byte> frame)
{
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(c.stream().get_executor(),
                 [&c, owned = std::move(owned)]
                 {
                     if(c.m_on_data_cb)
                         c.m_on_data_cb(std::span<const std::byte>{*owned});
                 });
}

template<typename Ch>
void serial_wire_decorator(Ch &c)
{
    c.m_decorator.on_match([&c](std::span<const std::byte> f) { serial_post_frame(c, f); });
    c.m_decorator.on_drop(
            [&c](wire::close_cause cause)
            {
                if(c.m_on_frame_dropped_cb)
                    c.m_on_frame_dropped_cb(cause);
            });
}

// Feed raw bytes to the CRC verify+resync decorator (a verified frame posts to on_data per the
// byte_channel posted-delivery contract); a read error routes through the channel's base fail().
template<typename Ch>
void serial_do_read(Ch &c)
{
    c.stream().async_read_some(::asio::buffer(c.m_read_buf),
                               [&c](std::error_code ec, std::size_t n)
                               {
                                   if(ec)
                                       return c.fail(ec);
                                   c.m_decorator.feed(std::span<const std::byte>{c.m_read_buf.data(), n});
                                   if(c.stream().is_open())
                                       serial_do_read(c);
                               });
}

}

#endif

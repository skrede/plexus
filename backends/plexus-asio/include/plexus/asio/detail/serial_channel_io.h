#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H

#include <asio/post.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <system_error>

namespace plexus::asio::detail {

// Feed raw bytes to the CRC verify+resync decorator (a verified frame posts to on_data per the
// byte_channel posted-delivery contract); a read error routes through the channel's base fail().
template<typename Ch>
void serial_do_read(Ch &c)
{
    c.serial_stream().async_read_some(::asio::buffer(c.serial_read_buf()),
                                      [&c](std::error_code ec, std::size_t n)
                                      {
                                          if(ec)
                                              return c.fail(ec);
                                          c.serial_decorator().feed(std::span<const std::byte>{c.serial_read_buf().data(), n});
                                          if(c.serial_stream().is_open())
                                              serial_do_read(c);
                                      });
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_CHANNEL_IO_H

#include <asio/post.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <system_error>

namespace plexus::asio::detail {

// The serial inbound read loop: it drives the CRC verify+resync decorator instead of
// feeding the shared stream_inbound directly (the TCP/unix path), so the byte-stable
// read-loop glue in stream_channel_io.h is left untouched (INV-1). Raw bytes go to the
// decorator; a CRC-verified complete frame is POSTED to on_data (the byte_channel posted-
// delivery contract); a corrupt frame fires the non-fatal on_frame_dropped seam from
// inside the decorator. A genuine read error routes through the channel's base fail()
// (network-drop on_error + on_closed), exactly as the shared loop does.
template<typename Ch>
void serial_do_read(Ch &c)
{
    c.serial_stream().async_read_some(::asio::buffer(c.serial_read_buf()),
                                      [&c](std::error_code ec, std::size_t n)
                                      {
                                          if(ec)
                                              return c.fail(ec);
                                          c.serial_decorator().feed(std::span<const std::byte>{
                                                  c.serial_read_buf().data(), n});
                                          if(c.serial_stream().is_open())
                                              serial_do_read(c);
                                      });
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H

#include "plexus/stream/crc_serial.h"

#include "plexus/wire_bytes.h"

#include <span>
#include <array>
#include <memory>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

// plaintext_bootstrap plus a CRC32C trailer on egress. submit queues the trailer as a SECOND
// gather node (no payload copy; it rides the same writev). arm_on_accept does NOT start a read
// loop: serial_channel owns and starts the inbound CRC verify+resync loop after its derived
// members are constructed.
template<typename Stream>
class serial_bootstrap
{
public:
    template<typename Io>
    static Stream make_stream(Io &io)
    {
        return Stream{io};
    }

    template<typename Io, typename Socket>
    static Stream make_stream(Io &, Socket connected)
    {
        return Stream{std::move(connected)};
    }

    template<typename Channel>
    void submit(Channel &c, std::span<const std::byte> data)
    {
        c.enqueue_egress(data);
        enqueue_trailer(c, data);
    }

    template<typename Channel>
    void submit(Channel &c, plexus::wire_bytes<> data)
    {
        const std::span<const std::byte> view = data;
        c.enqueue_egress_owned(std::move(data));
        enqueue_trailer(c, view);
    }

    template<typename Channel>
    void arm_on_accept(Channel &c)
    {
        c.mark_open();
    }

    void reset() noexcept
    {
    }

private:
    // The crc32c trailer is owned by a heap node so it stays resident across the gather-write.
    template<typename Channel>
    static void enqueue_trailer(Channel &c, std::span<const std::byte> framed)
    {
        const auto header  = framed.first(plexus::wire::header_size);
        const auto payload = framed.subspan(plexus::wire::header_size);
        auto owner         = std::make_shared<std::array<std::byte, plexus::stream::crc_trailer_size>>(plexus::stream::crc_trailer(header, payload));
        c.enqueue_egress_owned(plexus::wire_bytes<>{std::span<const std::byte>{*owner}, std::move(owner)});
    }
};

}

#endif

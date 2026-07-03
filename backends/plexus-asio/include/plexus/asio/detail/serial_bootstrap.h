#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H

#include "plexus/stream/crc_serial.h"

#include "plexus/wire_bytes.h"

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

// plaintext_bootstrap plus a CRC32C trailer on egress. submit admits the payload and its CRC32C
// trailer as one atomic unit — both frames or neither — so a full egress queue never leaves a
// payload on the wire without its trailer (nor a trailer alone); the two nodes ride the same
// writev. arm_on_accept does NOT start a read loop: serial_channel owns and starts the inbound
// CRC verify+resync loop after its derived members are constructed.
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
        auto owner = std::make_shared<std::vector<std::byte>>(data.begin(), data.end());
        plexus::wire_bytes<> payload{std::span<const std::byte>{*owner}, owner};
        c.enqueue_egress_pair(std::move(payload), make_trailer(data));
    }

    template<typename Channel>
    void submit(Channel &c, plexus::wire_bytes<> data)
    {
        // The trailer CRC is taken over the payload view BEFORE the move, so the owner is never
        // read after a rejected admission; payload and trailer then enter the queue as one unit.
        auto trailer = make_trailer(static_cast<std::span<const std::byte>>(data));
        c.enqueue_egress_pair(std::move(data), std::move(trailer));
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
    static plexus::wire_bytes<> make_trailer(std::span<const std::byte> framed)
    {
        const auto header  = framed.first(plexus::wire::header_size);
        const auto payload = framed.subspan(plexus::wire::header_size);
        auto owner         = std::make_shared<std::array<std::byte, plexus::stream::crc_trailer_size>>(plexus::stream::crc_trailer(header, payload));
        return plexus::wire_bytes<>{std::span<const std::byte>{*owner}, std::move(owner)};
    }
};

}

#endif

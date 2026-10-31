#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H

#include "plexus/stream/serial_frame.h"

#include "plexus/wire_bytes.h"

#include "plexus/wire/frame.h"

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

// plaintext_bootstrap plus the serial framing on egress. submit admits the serial header, the payload and the CRC32C
// trailer as one atomic unit, so a full egress queue never leaves part of a frame on the wire; the three nodes ride the
// same writev. arm_on_accept does NOT start a read loop: serial_channel owns and starts the inbound CRC verify+resync
// loop after its derived members are constructed.
template<typename Stream>
class serial_bootstrap
{
    struct envelope
    {
        std::array<std::byte, plexus::stream::serial_header_size> header;
        std::array<std::byte, plexus::stream::crc_trailer_size> trailer;
    };

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
        const auto payload = data.subspan(plexus::wire::header_size);
        auto owner         = std::make_shared<std::vector<std::byte>>(payload.begin(), payload.end());
        enqueue_framed(c, data.first(plexus::wire::header_size), plexus::wire_bytes<>{std::span<const std::byte>{*owner}, owner});
    }

    template<typename Channel>
    void submit(Channel &c, plexus::wire_bytes<> data)
    {
        const std::span<const std::byte> framed = data;
        enqueue_framed(c, framed.first(plexus::wire::header_size), plexus::wire_bytes<>{framed.subspan(plexus::wire::header_size), data.owner()});
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
    // The header and trailer share one heap node so they stay resident across the gather-write.
    template<typename Channel>
    static void enqueue_framed(Channel &c, std::span<const std::byte> wire_header, plexus::wire_bytes<> payload)
    {
        const auto header = plexus::stream::encode_serial_header(wire_header);
        auto owner        = std::make_shared<const envelope>(envelope{header, plexus::stream::crc_trailer(header, payload)});
        plexus::wire_bytes<> head{std::span<const std::byte>{owner->header}, owner};
        plexus::wire_bytes<> tail{std::span<const std::byte>{owner->trailer}, std::move(owner)};
        std::array<plexus::wire_bytes<>, 3> nodes{std::move(head), std::move(payload), std::move(tail)};
        c.enqueue_egress_unit(nodes);
    }
};

}

#endif

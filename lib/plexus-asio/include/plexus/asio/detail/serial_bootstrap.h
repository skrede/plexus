#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_SERIAL_BOOTSTRAP_H

#include "plexus/wire/crc_serial.h"

#include "plexus/wire_bytes.h"

#include <span>
#include <array>
#include <memory>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

// The serial open path: the plaintext bootstrap with a CRC32C integrity trailer spliced
// onto egress. make_stream/reset mirror plaintext_bootstrap. The two divergences keep the
// decorator splice OUT of the shared stream_channel files:
//   * submit appends the 4-byte LE CRC32C trailer as a SECOND queued gather node so the
//     payload is never copied to append it — the trailer rides the same single writev as
//     the framed bytes (the egress coalesces adjacent nodes), preserving zero-copy egress.
//   * arm_on_accept does NOT start a read loop: the inbound CRC verify+resync loop is owned
//     and started by serial_channel itself (after its derived members are constructed), not
//     by the base ctor — so the shared read-loop is bypassed without editing it.
template<typename Stream>
class serial_bootstrap
{
public:
    template<typename Io>
    [[nodiscard]] static Stream make_stream(Io &io)
    {
        return Stream{io};
    }

    template<typename Io, typename Socket>
    [[nodiscard]] static Stream make_stream(Io &, Socket connected)
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

    void reset() noexcept {}

private:
    // The framed bytes carry their own header+payload; the trailer is crc32c over them,
    // owned by a small heap node so it stays resident across the gather-write completion.
    template<typename Channel>
    static void enqueue_trailer(Channel &c, std::span<const std::byte> framed)
    {
        const auto header  = framed.first(plexus::wire::header_size);
        const auto payload = framed.subspan(plexus::wire::header_size);
        auto       owner = std::make_shared<std::array<std::byte, plexus::wire::crc_trailer_size>>(
                plexus::wire::crc_trailer(header, payload));
        c.enqueue_egress_owned(
                plexus::wire_bytes<>{std::span<const std::byte>{*owner}, std::move(owner)});
    }
};

}

#endif

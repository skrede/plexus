#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_PLAINTEXT_BOOTSTRAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_PLAINTEXT_BOOTSTRAP_H

#include "plexus/wire_bytes.h"

#include <span>
#include <utility>
#include <cstddef>

namespace plexus::asio::detail {

// The plaintext (TCP / AF_UNIX) open path: an accepted channel reads immediately and
// sends write straight to the bounded serial egress — there is no pre-data handshake to
// gate on. make_stream builds the bare socket; submit forwards to the egress with no
// buffering; arm_on_accept starts the read loop directly. The TLS bootstrap is the same
// shape with an open-before-data gate spliced between submit and the egress and the read loop
// armed by the handshake instead of at accept.
template <typename Stream>
class plaintext_bootstrap
{
public:
    template <typename Io>
    [[nodiscard]] static Stream make_stream(Io &io) { return Stream{io}; }

    template <typename Io, typename Socket>
    [[nodiscard]] static Stream make_stream(Io &io, Socket connected)
    {
        (void)io;
        return Stream{std::move(connected)};
    }

    template <typename Channel>
    void submit(Channel &c, std::span<const std::byte> data) { c.enqueue_egress(data); }

    // No pre-data handshake to gate on, so the owner rides straight to the serial egress
    // with no copy (the plaintext zero-copy send path).
    template <typename Channel>
    void submit(Channel &c, plexus::wire_bytes<> data) { c.enqueue_egress_owned(std::move(data)); }

    template <typename Channel>
    void arm_on_accept(Channel &c) { c.mark_open(); c.start_read_loop(); }

    void reset() noexcept {}
};

}

#endif

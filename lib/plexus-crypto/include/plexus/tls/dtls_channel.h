#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_CHANNEL_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_CHANNEL_H

#include "plexus/io/byte_channel.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include "plexus/wire/stream_inbound.h"

#include <span>
#include <utility>
#include <cstddef>

namespace plexus::tls {

// byte_channel for the secure-best_effort DTLS datagram transport: a per-peer
// facade over the shared UDP socket, driving an OpenSSL DTLS state machine through
// a BIO pair (there is no asio DTLS stream wrapper, so the record pump lives in the
// .cpp with raw OpenSSL). Single-owner, bare `this`, posted on_data — NO
// shared_from_this, NO strand.
//
// This declaration is the compile-only byte_channel seam the dtls_policy bundle
// pins; the OpenSSL BIO-pair pump, the cookie-gated handshake, the retransmit
// timer, and the external-complete edge are implemented alongside it in the gated
// crypto TU. Until that engine lands the surface is declaration-only (no inline
// bodies) so the Policy concept gate holds without pulling OpenSSL into consumer
// translation units.
class dtls_channel
{
public:
    void send(std::span<const std::byte> bytes);
    void close();

    [[nodiscard]] io::endpoint remote_endpoint() const;

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb);
    void on_closed(plexus::detail::move_only_function<void()> cb);
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb);
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb);
};

}

static_assert(plexus::io::byte_channel<plexus::tls::dtls_channel>,
    "dtls_channel must satisfy byte_channel — check the send/close/on_* surface");

#endif

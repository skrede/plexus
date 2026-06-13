#ifndef HPP_GUARD_PLEXUS_TLS_TLS_CHANNEL_H
#define HPP_GUARD_PLEXUS_TLS_TLS_CHANNEL_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_bootstrap.h"

#include "plexus/asio/stream_channel.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/byte_channel.h"
#include "plexus/detail/compat.h"

#include <asio/ip/tcp.hpp>
#include <asio/ssl/stream.hpp>
#include <asio/io_context.hpp>

#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::tls {

// The TLS byte_channel: stream_channel over ::asio::ssl::stream<tcp::socket> with the TLS
// open path — the read loop arms only on handshake success (ciphertext is never read as
// frames) and sends route through the bootstrap's open-before-data handshake_gate (buffered
// pre-handshake, drained on mark_ready) so no plaintext is written before the secure channel
// is up. socket() returns the lowest (TCP) layer the transport async_connects. The handshake
// is started by the transport/listener via start_client_handshake / start_server_handshake.
struct tls_traits
{
    using stream_type = ::asio::ssl::stream<::asio::ip::tcp::socket>;
    using lowest_layer_type = stream_type::lowest_layer_type;

    static io::endpoint format_endpoint(const stream_type &stream)
    {
        std::error_code ec;
        auto ep = stream.lowest_layer().remote_endpoint(ec);
        if(ec)
            return {"tls", ""};
        return {"tls", ep.address().to_string() + ":" + std::to_string(ep.port())};
    }

    static void shutdown(lowest_layer_type &sock, std::error_code &ec)
    {
        (void)sock.shutdown(::asio::ip::tcp::socket::shutdown_both, ec);
    }

    static lowest_layer_type &lowest_layer(stream_type &s) noexcept { return s.lowest_layer(); }
    static const lowest_layer_type &lowest_layer(const stream_type &s) noexcept { return s.lowest_layer(); }

    // The TLS lowest layer is the TCP socket, so the buffer + keepalive knobs apply through
    // the same portable/granular path the TCP channel uses (the read loop arms only post-
    // handshake, so the knobs land on the live TCP socket).
    static void apply_socket_options(lowest_layer_type &sock,
                                     const plexus::asio::stream_socket_options &opts, std::error_code &ec)
    {
        plexus::asio::tcp_traits::apply_socket_options(sock, opts, ec);
    }
};

class tls_channel
    : public plexus::asio::stream_channel<::asio::ssl::stream<::asio::ip::tcp::socket>,
                                          tls_traits, detail::tls_bootstrap<::asio::ssl::stream<::asio::ip::tcp::socket>>>
{
    using stream_type = ::asio::ssl::stream<::asio::ip::tcp::socket>;
    using base = plexus::asio::stream_channel<stream_type, tls_traits, detail::tls_bootstrap<stream_type>>;

public:
    static constexpr std::size_t default_outbox_bytes = base::default_write_queue_bytes;

    // Dial mode: unconnected ssl::stream. The transport async_connects the lowest layer, then
    // calls start_client_handshake(host). The credential is REQUIRED (first after io).
    tls_channel(::asio::io_context &io, const tls_credential &cred,
                wire::stream_inbound_config cfg = {},
                io::congestion congestion = io::congestion::block,
                std::size_t outbox_bytes = default_outbox_bytes,
                plexus::asio::stream_socket_options opts = {})
        : base(io, cfg, congestion, outbox_bytes, opts, cred)
    {
    }

    // Accept mode: adopt an already-connected tcp::socket into a server-side ssl::stream. The
    // server handshake is NOT started here — the listener wires its readiness hook and then
    // calls start_server_handshake(), so the accepted channel is delivered POST-handshake (a
    // verify-rejected peer never yields a live accepted channel — fail-closed, symmetric with
    // the dial side).
    tls_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected,
                const tls_credential &cred, wire::stream_inbound_config cfg = {},
                io::congestion congestion = io::congestion::block,
                std::size_t outbox_bytes = default_outbox_bytes,
                plexus::asio::stream_socket_options opts = {})
        : base(io, std::move(connected), cfg, congestion, outbox_bytes, opts, cred)
    {
    }

    void start_client_handshake(const std::string &host,
                                plexus::detail::move_only_function<void()> on_ready = {})
    {
        bootstrap().start_client_handshake(*this, host, std::move(on_ready));
    }

    void start_server_handshake(plexus::detail::move_only_function<void()> on_ready = {})
    {
        bootstrap().start_server_handshake(*this, std::move(on_ready));
    }
};

}

static_assert(plexus::io::byte_channel<plexus::tls::tls_channel>,
    "tls_channel must satisfy byte_channel — check the seven verbs");

#endif

#ifndef HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H

#include "plexus/tls/tls_policy.h"
#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_listener.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_context.h"

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>

#include <array>
#include <memory>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::tls {

// The TLS connector: a protocol-type swap of the plaintext transport with a
// handshake hop. wraps tls_listener for listen + accept and adds an
// async-connect-then-client-handshake dial over the ssl::stream. listen(ep)
// starts the acceptor and forwards each accepted (server-handshaking) channel
// through on_accepted; dial(ep) parses the host:port target, async-connects a
// fresh client channel, then runs the client handshake and hands the live
// channel to on_dialed ONLY POST-handshake — so a verify-rejected peer never
// yields a live channel (fail-closed). A handshake/verify failure surfaces as
// on_dial_failed via the channel's own error path. The credential is REQUIRED,
// non-defaultable (first after io); cfg stays required-WITH-default. The dialed
// endpoint rides the async closure so on_dialed / on_dial_failed CARRY it back
// (the engine correlates each completion to its slot by endpoint).
class tls_transport
{
public:
    tls_transport(::asio::io_context &io, const tls_credential &cred,
                  wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_cred(cred)
        , m_listener(io, cred, cfg)
        , m_cfg(cfg)
    {
        m_listener.on_accepted([this](std::unique_ptr<tls_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(std::move(ch));
        });
        m_listener.on_error([this](io::io_error e) {
            if(m_on_error)
                m_on_error(e);
        });
    }

    tls_transport(const tls_transport &) = delete;
    tls_transport &operator=(const tls_transport &) = delete;

    // The concrete channel this member's completions deliver + its routing identity: the
    // schemes it serves and the locality tier. TLS-over-TCP is a remote (encrypted stream)
    // member serving the "tls" scheme — it rides a network path, so locality confinement
    // still excludes it. A generic multiplexer reads these at compile time to route over a pack.
    using channel_type = tls_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"tls"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep) { m_listener.start(ep); }

    [[nodiscard]] uint16_t port() const { return m_listener.port(); }

    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto target = plexus::asio::detail::parse(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, plexus::asio::detail::map_error(pec));
        auto ch = std::make_unique<tls_channel>(m_io, m_cred, m_cfg);
        auto &raw = *ch;
        raw.socket().async_connect(target,
            [this, ep, ch = std::move(ch)](std::error_code ec) mutable {
                if(ec)
                    return report_dial_fail(ep, plexus::asio::detail::map_error(ec));
                run_handshake(std::move(ch), ep);
            });
    }

    void close() { m_listener.stop(); }

private:
    // Run the client handshake; deliver to on_dialed only on success. The
    // channel is kept alive across the handshake by the on_ready closure (which
    // owns the unique_ptr); a handshake/verify failure routes through the
    // channel's on_error to on_dial_failed (fail-closed).
    void run_handshake(std::unique_ptr<tls_channel> ch, const io::endpoint &ep)
    {
        auto &raw = *ch;
        raw.on_error([this, ep](io::io_error e) { report_dial_fail(ep, e); });
        auto host = detail::sni_host(ep.address);
        raw.start_client_handshake(host,
            [this, ep, ch = std::move(ch)]() mutable {
                // The handshake succeeded — the dial-failure error hook is no
                // longer the right target; the consumer re-wires on_error when
                // it adopts the channel from on_dialed (the established
                // adopt-then-wire contract).
                if(m_on_dialed)
                    m_on_dialed(std::move(ch), ep);
            });
    }

    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    ::asio::io_context &m_io;
    const tls_credential &m_cred;
    tls_listener m_listener;
    wire::stream_inbound_config m_cfg;
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::tls_transport, plexus::tls::tls_policy>,
    "tls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

#ifndef HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H
#define HPP_GUARD_PLEXUS_TLS_TLS_TRANSPORT_H

#include "plexus/tls/tls_policy.h"
#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_listener.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/detail/tls_context.h"
#include "plexus/tls/detail/tls_transport_accept.h"

#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/pending_dial_registry.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <array>
#include <memory>
#include <variant>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::tls {

// The TLS connector: a protocol-type swap of the plaintext transport with a handshake hop. It
// wraps tls_listener for listen + accept and adds an async-connect-then-client-handshake dial over
// the ssl::stream. dial(ep) async-connects a fresh client channel then runs the client handshake,
// handing the live channel to on_dialed ONLY POST-handshake (a verify-rejected peer never yields a
// live channel — fail-closed). Each half-open dial is owned by the transport's
// pending_dial_registry (copy-before-erase resolve, deferred-destroy fail), so a fail edge firing
// from inside the channel's async stack frees it OFF that stack. The credential is REQUIRED
// (non-defaultable, first after io); the dialed endpoint rides the async closure so on_dialed /
// on_dial_failed carry it back for the engine's by-endpoint correlation.
class tls_transport
{
public:
    // no_delay disables Nagle on every dialed + accepted TCP lowest layer (required-WITH-default
    // true). global_default is the per-MESSAGE receive ceiling and reassembly_budget the aggregate
    // reassembly-memory cap (the two operator-facing message-size node options, stamped onto every
    // minted channel's inbound config). All required-WITH-default.
    tls_transport(::asio::io_context &io, const tls_credential &cred,
                  stream::stream_inbound_config cfg = {}, bool no_delay = true,
                  io::congestion      congestion = io::congestion::block,
                  io::egress_capacity egress     = io::egress_capacity::bounded_default(),
                  plexus::asio::stream_socket_options socket_options = {},
                  std::size_t global_default    = io::global_default_max_message_bytes,
                  std::size_t reassembly_budget = io::reassembly_memory_budget)
            : m_io(io)
            , m_cred(cred)
            , m_listener(io, cred,
                         stream::with_message_limits(cfg, global_default, reassembly_budget),
                         no_delay, congestion, egress, socket_options)
            , m_cfg(stream::with_message_limits(cfg, global_default, reassembly_budget))
            , m_no_delay(no_delay)
            , m_congestion(congestion)
            , m_egress_capacity(egress)
            , m_socket_options(socket_options)
            // Destroy a freed channel OFF the current stack: a posted continuation owns it until
            // it runs, so teardown follows the channel's own async-completion unwind.
            , m_pending(
                      [this](std::unique_ptr<tls_channel> ch)
                      { ::asio::post(m_io, [owned = std::move(ch)]() mutable { owned.reset(); }); })
    {
        m_listener.on_accepted(
                [this](std::unique_ptr<tls_channel> ch)
                {
                    if(m_on_accepted)
                        m_on_accepted(std::move(ch));
                });
        m_listener.on_error(
                [this](io::io_error e)
                {
                    if(m_on_error)
                        m_on_error(e);
                });
    }

    tls_transport(const tls_transport &)            = delete;
    tls_transport &operator=(const tls_transport &) = delete;

    // The concrete channel + routing identity: TLS-over-TCP is a remote member serving the "tls"
    // scheme (a network path, so locality confinement excludes it). Read at compile time to route.
    using channel_type = tls_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"tls"};
    static constexpr io::transport_kind              mux_tier = io::transport_kind::remote;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>,
                                                           const io::endpoint &)>
                           cb)
    {
        m_on_dialed = std::move(cb);
    }
    void
    on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const io::endpoint &ep) { m_listener.start(ep); }

    [[nodiscard]] uint16_t port() const { return m_listener.port(); }

    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto            target = plexus::asio::detail::parse(ep.address, pec);
        if(pec)
            return detail::tls_report_dial_fail(*this, ep, plexus::asio::detail::map_error(pec));
        auto  ch  = std::make_unique<tls_channel>(m_io, m_cred, m_cfg, m_congestion,
                                                  m_egress_capacity, m_socket_options);
        auto *raw = ch.get();
        raw->socket().async_connect(
                target,
                [this, ep, ch = std::move(ch), raw](std::error_code ec) mutable
                {
                    if(ec)
                        return detail::tls_report_dial_fail(*this, ep,
                                                            plexus::asio::detail::map_error(ec));
                    if(m_no_delay)
                    {
                        std::error_code nec;
                        (void)raw->socket().set_option(::asio::ip::tcp::no_delay(true),
                                                       nec); // disable Nagle pre-handshake
                    }
                    detail::tls_run_handshake(*this, std::move(ch), raw, ep);
                });
    }

    void close()
    {
        m_pending.clear();
        m_listener.stop();
    }

private:
    // The connect/handshake-pump + dial-resolution glue is relocated to
    // detail/tls_transport_accept.h (relocation by friendship): each helper reaches the
    // listener/credential/pending-dial members below through the transport reference.
    template<typename U>
    friend void detail::tls_report_dial_fail(U &, const io::endpoint &, io::io_error);
    template<typename U>
    friend void detail::tls_resolve_dial(U &, const io::endpoint &, tls_channel *);
    template<typename U>
    friend void detail::tls_fail_dial(U &, const io::endpoint &, tls_channel *, io::io_error);
    template<typename U>
    friend void detail::tls_run_handshake(U &, std::unique_ptr<tls_channel>, tls_channel *,
                                          const io::endpoint &);

    ::asio::io_context                 &m_io;
    const tls_credential               &m_cred;
    tls_listener                        m_listener;
    stream::stream_inbound_config         m_cfg;
    bool                                m_no_delay;
    io::congestion                      m_congestion;
    io::egress_capacity                 m_egress_capacity;
    plexus::asio::stream_socket_options m_socket_options;
    io::pending_dial_registry<tls_channel, std::monostate>
            m_pending; // transport-owned half-open dials
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<tls_channel>, const io::endpoint &)>
                                                                                 m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)>                       m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::tls::tls_transport, plexus::tls::tls_policy>,
              "tls_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

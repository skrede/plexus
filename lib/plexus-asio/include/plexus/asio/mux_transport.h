#ifndef HPP_GUARD_PLEXUS_ASIO_MUX_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_MUX_TRANSPORT_H

#include "plexus/asio/mux_policy.h"
#include "plexus/asio/mux_channel.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/mux_selector.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/detail/compat.h"

#include <memory>
#include <cstdint>
#include <utility>

namespace plexus::asio {

// The multiplexing transport_backend: it owns a local (AF_UNIX), a remote (plain TCP),
// a secure (TLS-over-TCP), and a datagram (connectionless UDP) concrete transport plus a
// transport_selector, and presents the single transport_backend surface the engine drives
// unchanged. On dial(ep) it consults the selector for the tier, then within the remote
// tier discriminates the wire protocol by deriving the reliability class from the scheme
// (reliability_of_scheme): best_effort "udp" -> the datagram member, "tls" -> the secure
// member, reliable "tcp" -> the plain-TCP member; and the reliable-datagram "udpr" -> the
// SLICE-state guard below. It delegates to ONE chosen concrete transport; that transport's
// completion (wired once in the ctor) wraps its concrete channel in a mux_channel and
// re-emits it to the engine with the SAME ep — so the registry's existing endpoint-
// correlation demuxes mux completions with zero engine/registry change. listen(ep) routes
// the same way, so a mux node accepts on all families by listening once per family. An
// accepted channel's scheme survives the erasure (the wrap never touches remote_endpoint()):
// a unix-listener accept reports "unix", a tcp-listener "tcp", a tls-listener "tls", a
// udp-listener "udp".
//
// RELIABLE-DATAGRAM GUARD (a TEMPORARY routing, not a permanent one): the "udpr" scheme
// classifies reliable_datagram, but the datagram-with-retransmit ARQ does not exist yet.
// Until it lands, a "udpr" dial routes to the reliable STREAM member (plain TCP) — NEVER to
// the bare best_effort UDP member, which would silently serve a reliable request lossily (a
// reliability downgrade). When the datagram ARQ engine exists, this guard is retargeted so
// "udpr" rides the UDP member. The standing invariant: reliable_datagram is never served
// over bare best_effort UDP.
//
// KNOWN TENSION (a deferred strategic fork): the UDP member is the 4th MANDATORY borrowed
// member, extending the fixed-tuple coupling. Whether the mux composes a variable member
// set instead is a deliberately deferred operator decision; this phase faithfully extends
// the status-quo fixed-member shape — it does NOT redesign the composition model.
//
// LIFETIME: the ctor installs this-capturing completion callbacks into the borrowed local,
// remote, secure, and datagram transports. The borrowed transports MUST outlive this object
// — the owner sequences teardown so the mux is destroyed before any transport fires a late
// completion (the same caller-owned discipline asio_transport documents; no shared lifetime
// handle is taken). The mux only BORROWS the secure transport; it does not mint or own the
// credential — the tls_transport is constructed by its owner with the required credential
// (just like the local/remote/datagram members).
class multiplexing_transport
{
public:
    multiplexing_transport(unix_transport &local, asio_transport &remote, tls::tls_transport &secure,
                           udp_transport &datagram, transport_selector selector = {})
        : m_local(local)
        , m_remote(remote)
        , m_secure(secure)
        , m_datagram(datagram)
        , m_selector(selector)
    {
        m_local.on_accepted([this](std::unique_ptr<unix_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(wrap(std::move(ch)));
        });
        m_remote.on_accepted([this](std::unique_ptr<asio_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(wrap(std::move(ch)));
        });
        m_local.on_dialed([this](std::unique_ptr<unix_channel> ch, const io::endpoint &ep) {
            if(m_on_dialed)
                m_on_dialed(wrap(std::move(ch)), ep);
        });
        m_remote.on_dialed([this](std::unique_ptr<asio_channel> ch, const io::endpoint &ep) {
            if(m_on_dialed)
                m_on_dialed(wrap(std::move(ch)), ep);
        });
        m_local.on_dial_failed([this](const io::endpoint &ep, io::io_error e) {
            if(m_on_dial_failed)
                m_on_dial_failed(ep, e);
        });
        m_remote.on_dial_failed([this](const io::endpoint &ep, io::io_error e) {
            if(m_on_dial_failed)
                m_on_dial_failed(ep, e);
        });
        m_secure.on_accepted([this](std::unique_ptr<tls::tls_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(wrap(std::move(ch)));
        });
        m_secure.on_dialed([this](std::unique_ptr<tls::tls_channel> ch, const io::endpoint &ep) {
            if(m_on_dialed)
                m_on_dialed(wrap(std::move(ch)), ep);
        });
        m_secure.on_dial_failed([this](const io::endpoint &ep, io::io_error e) {
            if(m_on_dial_failed)
                m_on_dial_failed(ep, e);
        });
        m_datagram.on_accepted([this](std::unique_ptr<udp_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(wrap(std::move(ch)));
        });
        m_datagram.on_dialed([this](std::unique_ptr<udp_channel> ch, const io::endpoint &ep) {
            if(m_on_dialed)
                m_on_dialed(wrap(std::move(ch)), ep);
        });
        m_datagram.on_dial_failed([this](const io::endpoint &ep, io::io_error e) {
            if(m_on_dial_failed)
                m_on_dial_failed(ep, e);
        });
        m_local.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
        m_remote.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
        m_secure.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
        m_datagram.on_error([this](io::io_error e) { if(m_on_error) m_on_error(e); });
    }

    multiplexing_transport(const multiplexing_transport &) = delete;
    multiplexing_transport &operator=(const multiplexing_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<mux_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<mux_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    // Route the listen to the concrete transport whose family matches the endpoint scheme;
    // a mux node listening on every family calls listen once per family. The route is
    // derived from the endpoint alone (the same composition dial() uses).
    void listen(const io::endpoint &ep)
    {
        switch(route_of(ep))
        {
            case route::local:    return m_local.listen(ep);
            case route::secure:   return m_secure.listen(ep);
            case route::datagram: return m_datagram.listen(ep);
            case route::stream:   return m_remote.listen(ep);
        }
    }

    // Select ONE transport per peer at dial time, then delegate; the chosen transport's
    // on_dialed (wired in the ctor) re-emits the wrapped channel + the SAME ep. The
    // endpoint is passed through unchanged — never rewritten — so the engine's endpoint-
    // correlation cannot misroute the completion.
    void dial(const io::endpoint &ep)
    {
        switch(route_of(ep))
        {
            case route::local:    return m_local.dial(ep);
            case route::secure:   return m_secure.dial(ep);
            case route::datagram: return m_datagram.dial(ep);
            case route::stream:   return m_remote.dial(ep);
        }
    }

    void close() { m_local.close(); m_remote.close(); m_secure.close(); m_datagram.close(); }

private:
    enum class route : std::uint8_t { local, stream, secure, datagram };

    // The 4-way composition, derived from ep ALONE (the dial(ep) reality): locality wins
    // (same-host -> the local stream); otherwise the remote tier is split by the scheme's
    // reliability class. best_effort "udp" -> the datagram member; "tls" -> the secure
    // member; reliable "tcp" -> the plain-TCP stream member; reliable_datagram "udpr" -> the
    // GUARD: route to the reliable STREAM member (TCP) — NEVER to bare best_effort UDP —
    // until the datagram ARQ exists, so a reliable request is never silently served lossily.
    [[nodiscard]] route route_of(const io::endpoint &ep) const noexcept
    {
        if(m_selector.select(ep, reliability_hint::unspecified) == transport_kind::local)
            return route::local;
        if(ep.scheme == "tls")
            return route::secure;
        switch(m_selector.reliability_of_scheme(ep.scheme))
        {
            case reliability_hint::best_effort:       return route::datagram;
            case reliability_hint::reliable_datagram: return route::stream;   // guard: no downgrade to UDP
            default:                                  return route::stream;   // reliable/unspecified remote
        }
    }

    template <typename C>
    static std::unique_ptr<mux_channel> wrap(std::unique_ptr<C> ch)
    {
        return std::make_unique<mux_channel>(std::make_unique<channel_adapter<C>>(std::move(ch)));
    }

    unix_transport &m_local;
    asio_transport &m_remote;
    tls::tls_transport &m_secure;
    udp_transport &m_datagram;
    transport_selector m_selector;
    plexus::detail::move_only_function<void(std::unique_ptr<mux_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<mux_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::multiplexing_transport, plexus::asio::mux_policy>,
    "multiplexing_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

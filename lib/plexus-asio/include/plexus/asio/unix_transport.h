#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_TRANSPORT_H

#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>

#include <array>
#include <memory>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::asio {

// The AF_UNIX connector: a protocol-type swap of asio_transport. wraps unix_listener
// for listen + accept and adds an async-connect dial over the local-stream socket.
// listen(ep) starts the acceptor and forwards each accepted channel through
// on_accepted; dial(ep) parses the target path, async-connects a fresh client
// channel, and on success hands the live channel to on_dialed (on failure fires
// on_dial_failed with a mapped io_error). The connect closure OWNS the channel for
// the duration of the async op (the move_only_function holds the unique_ptr) — no
// shared lifetime handle is taken; the unix_transport owner must outlive any pending
// connect, the same caller-owned lifetime discipline unix_channel and unix_listener
// document. close() stops the acceptor.
class unix_transport
{
public:
    // cfg is the node-level byte-stream hardening config (required-WITH-default):
    // the transport mints every stream channel — accepted via the listener and
    // dialed — with it, so the no-progress/slowloris defense is armed structurally
    // (no setter, no sentinel; the defaults live in stream_inbound_config itself).
    explicit unix_transport(::asio::io_context &io, wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_listener(io, cfg)
        , m_cfg(cfg)
    {
        m_listener.on_accepted([this](std::unique_ptr<unix_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(std::move(ch));
        });
        m_listener.on_error([this](io::io_error e) {
            if(m_on_error)
                m_on_error(e);
        });
    }

    unix_transport(const unix_transport &) = delete;
    unix_transport &operator=(const unix_transport &) = delete;

    // The concrete channel this member's completions deliver + its routing identity:
    // the schemes it serves and the locality tier it belongs to. A generic multiplexer
    // reads these at compile time to route by scheme over a member pack — AF_UNIX is the
    // same-host (local) member, serving the "unix" scheme.
    using channel_type = unix_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"unix"};
    static constexpr io::transport_kind mux_tier = io::transport_kind::local;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>, const io::endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep) { m_listener.start(ep); }

    // The dialed endpoint rides the async closure so on_dialed / on_dial_failed
    // CARRY it back: the engine correlates each completion to its slot by endpoint,
    // not by arrival order (concurrent dials over one transport reorder).
    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto target = detail::parse_unix(ep.address, pec);
        if(pec)
            return report_dial_fail(ep, detail::map_error(pec));
        auto ch = std::make_unique<unix_channel>(m_io, m_cfg);
        auto &raw = *ch;
        raw.socket().async_connect(target,
            [this, ep, ch = std::move(ch)](std::error_code ec) mutable {
                if(ec)
                    return report_dial_fail(ep, detail::map_error(ec));
                ch->start_read();
                if(m_on_dialed)
                    m_on_dialed(std::move(ch), ep);
            });
    }

    void close() { m_listener.stop(); }

private:
    void report_dial_fail(const io::endpoint &ep, io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(ep, e);
    }

    ::asio::io_context &m_io;
    unix_listener m_listener;
    wire::stream_inbound_config m_cfg;
    plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>, const io::endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::unix_transport, plexus::asio::unix_policy>,
    "unix_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

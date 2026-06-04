#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_TRANSPORT_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_endpoint_parse.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>

#include <memory>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::asio {

// The asio connector: wraps the existing asio_listener for listen + accept and
// adds an async-connect dial. listen(ep) starts the acceptor and forwards each
// accepted channel through on_accepted; dial(ep) parses the target, async-connects
// a fresh client channel, and on success hands the live channel to on_dialed (on
// failure fires on_dial_failed with a mapped io_error). The connect closure OWNS
// the channel for the duration of the async op (the move_only_function holds the
// unique_ptr) — no shared lifetime handle is taken; the asio_transport owner must
// outlive any pending connect, the same caller-owned lifetime discipline
// asio_channel and asio_listener document. close() stops the acceptor.
class asio_transport
{
public:
    explicit asio_transport(::asio::io_context &io)
        : m_io(io)
        , m_listener(io)
    {
        m_listener.on_accepted([this](std::unique_ptr<asio_channel> ch) {
            if(m_on_accepted)
                m_on_accepted(std::move(ch));
        });
        m_listener.on_error([this](io::io_error e) {
            if(m_on_error)
                m_on_error(e);
        });
    }

    asio_transport(const asio_transport &) = delete;
    asio_transport &operator=(const asio_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const io::endpoint &ep) { m_listener.start(ep); }

    void dial(const io::endpoint &ep)
    {
        std::error_code pec;
        auto target = detail::parse(ep.address, pec);
        if(pec)
            return report_dial_fail(detail::map_error(pec));
        auto ch = std::make_unique<asio_channel>(m_io);
        auto &raw = *ch;
        raw.socket().async_connect(target,
            [this, ch = std::move(ch)](std::error_code ec) mutable {
                if(ec)
                    return report_dial_fail(detail::map_error(ec));
                ch->start_read();
                if(m_on_dialed)
                    m_on_dialed(std::move(ch));
            });
    }

    void close() { m_listener.stop(); }

    [[nodiscard]] uint16_t port() const { return m_listener.port(); }

private:
    void report_dial_fail(io::io_error e)
    {
        if(m_on_dial_failed)
            m_on_dial_failed(e);
    }

    ::asio::io_context &m_io;
    asio_listener m_listener;
    plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<asio_channel>)> m_on_dialed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
};

}

static_assert(plexus::io::transport_backend<plexus::asio::asio_transport, plexus::asio::asio_policy>,
    "asio_transport must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

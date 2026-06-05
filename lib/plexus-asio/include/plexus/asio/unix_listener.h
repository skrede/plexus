#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <memory>
#include <cerrno>
#include <utility>
#include <system_error>

namespace plexus::asio {

// AF_UNIX acceptor over ::asio::local::stream_protocol::acceptor. A protocol-type
// swap of asio_listener plus the socket-file lifecycle — the only genuinely new
// mechanics here. start(endpoint) parses the path, unlinks a STALE socket file
// left by a crashed prior run (so bind cannot wedge with EADDRINUSE), opens, binds,
// chmods the socket owner-only (0700), and listens, then loops accepting; each
// accepted socket is adopted by a fresh unix_channel (already reading) and handed
// to on_accepted as a unique_ptr owner. stop() removes the filesystem entry the
// acceptor close leaves behind, but only a path THIS listener bound. There is NO
// reuse_address (AF_UNIX has none) and NO port (the rendezvous is the path).
class unix_listener
{
public:
    // cfg is the node-level hardening config (required-WITH-default), stamped onto
    // every channel this listener accepts so each minted stream arms it structurally.
    explicit unix_listener(::asio::io_context &io, wire::stream_inbound_config cfg = {})
        : m_io(io)
        , m_acceptor(io)
        , m_cfg(cfg)
    {
    }

    unix_listener(const unix_listener &) = delete;
    unix_listener &operator=(const unix_listener &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    void start(const io::endpoint &bind_ep)
    {
        std::error_code ec;
        auto ep = detail::parse_unix(bind_ep.address, ec);
        if(ec)
            return report(ec);
        const auto &path = bind_ep.address;
        unlink_path(path);                        // clear a stale socket file before bind
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return report(ec);
        m_acceptor.bind(ep, ec);                  // NO reuse_address for AF_UNIX
        if(ec)
            return report(ec);
        if(::chmod(path.c_str(), S_IRWXU) != 0)   // owner-only 0700 access control
            return report(std::error_code(errno, std::generic_category()));
        m_acceptor.listen(::asio::socket_base::max_listen_connections, ec);
        if(ec)
            return report(ec);
        m_bound_path = path;
        m_running = true;
        do_accept();
    }

    void stop()
    {
        m_running = false;
        std::error_code ec;
        (void)m_acceptor.cancel(ec);
        (void)m_acceptor.close(ec);
        if(!m_bound_path.empty())                 // only unlink a path THIS listener bound
        {
            unlink_path(m_bound_path);
            m_bound_path.clear();
        }
    }

private:
    // Remove a socket file, treating ENOENT (nothing there) as success.
    static void unlink_path(const std::string &path)
    {
        if(::unlink(path.c_str()) != 0 && errno != ENOENT)
            return;   // a non-ENOENT failure surfaces later via bind/listen
    }

    void do_accept()
    {
        m_acceptor.async_accept(
            [this](std::error_code ec, ::asio::local::stream_protocol::socket peer)
            {
                if(ec)
                {
                    if(ec != ::asio::error::operation_aborted)
                        report(ec);
                    return;
                }
                auto channel = std::make_unique<unix_channel>(m_io, std::move(peer), m_cfg);
                if(m_on_accepted)
                    m_on_accepted(std::move(channel));
                if(m_running)
                    do_accept();
            });
    }

    void report(const std::error_code &ec)
    {
        if(m_on_error)
            m_on_error(detail::map_error(ec));
    }

    ::asio::io_context &m_io;
    ::asio::local::stream_protocol::acceptor m_acceptor;
    wire::stream_inbound_config m_cfg;
    std::string m_bound_path;
    plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_running{false};
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/detail/unix_accept.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/egress_capacity.h"
#include "plexus/io/security/peer_cred_policy.h"
#include "plexus/detail/compat.h"
#include "plexus/detail/socket_compat.h"

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <string>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::asio {

// AF_UNIX acceptor over ::asio::local::stream_protocol::acceptor. start(endpoint) unlinks a STALE
// socket file left by a crashed prior run (so bind cannot wedge with EADDRINUSE), binds under a
// restrictive umask so the inode is created owner-only with no post-bind chmod window, then chmods
// to the configured mode and listens. stop() unlinks only a path THIS listener bound.
//
// SECURITY CONTRACT (caller-owned): the socket path MUST live in a directory the owner controls
// (not a world-writable shared tmp), so the unlink-before-bind cannot be raced. The socket mode
// (0700 default, a widened knob) is the access boundary; the injected peer_cred_policy is
// defense-in-depth ABOVE the mode, judging {uid, gid, pid} at accept and refusing an unauthorized
// local peer fail-closed.
class unix_listener
{
public:
    // The fail-closed boundary. The knob widens deliberately (e.g. 0770) — loosening is an
    // informed choice, never a default.
    static constexpr auto default_socket_mode = plexus::detail::default_socket_mode;

    explicit unix_listener(::asio::io_context &io, stream::stream_inbound_config cfg = {}, plexus::detail::socket_mode mode = default_socket_mode,
                           const io::security::peer_cred_policy &policy = io::security::shared_accept_any_peer_cred(), io::congestion congestion = io::congestion::block,
                           io::egress_capacity egress = io::egress_capacity::bounded_default(), stream_socket_options socket_options = {})
            : m_io(io)
            , m_acceptor(io)
            , m_cfg(cfg)
            , m_mode(mode)
            , m_peer_policy(&policy)
            , m_congestion(congestion)
            , m_egress_capacity(egress)
            , m_socket_options(socket_options)
    {
    }

    unix_listener(const unix_listener &)            = delete;
    unix_listener &operator=(const unix_listener &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> cb)
    {
        m_on_accepted_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    ~unix_listener()
    {
        stop();
    }

    // NOLINTNEXTLINE(readability-function-size)
    void start(const io::endpoint &bind_ep)
    {
        std::error_code ec;
        auto ep = detail::parse_unix(bind_ep.address, ec);
        if(ec)
            return detail::report(*this, ec);
        // Windows AF_UNIX exposes no peer credentials. A non-accept_any policy cannot be
        // honored, so refuse at listen (fail-closed) rather than silently admit every peer.
        if constexpr(!plexus::detail::peer_cred_supported)
        {
            if(!m_peer_policy->accepts_without_credentials())
                return detail::report(*this, std::make_error_code(std::errc::operation_not_supported));
        }
        const auto &path = bind_ep.address;
        plexus::detail::remove_socket_path(path); // clear a stale socket file before bind
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return detail::report(*this, ec);
        {
            plexus::detail::scoped_bind_umask umask_guard;
            m_acceptor.bind(ep, ec); // NO reuse_address for AF_UNIX
        }
        if(ec)
            return detail::abort_start(*this, ec); // close the opened acceptor
        m_bound_path = path;                       // bind created the inode — own it so any later failure unlinks it
        if(!plexus::detail::apply_socket_mode(path, m_mode, ec)) // 0700 default, or a widened knob
            return detail::abort_start(*this, ec);
        m_acceptor.listen(::asio::socket_base::max_listen_connections, ec);
        if(ec)
            return detail::abort_start(*this, ec);
        m_running = true;
        detail::do_accept(*this);
    }

    void stop()
    {
        m_running = false;
        std::error_code ec;
        (void)m_acceptor.cancel(ec);
        (void)m_acceptor.close(ec);
        if(!m_bound_path.empty()) // only unlink a path THIS listener bound
        {
            plexus::detail::remove_socket_path(m_bound_path);
            m_bound_path.clear();
        }
    }

private:
    template<typename L>
    friend void detail::report(L &, const std::error_code &);
    template<typename L>
    friend void detail::abort_start(L &, const std::error_code &);
    template<typename L>
    friend bool detail::admit_peer(const L &, ::asio::local::stream_protocol::socket &);
    template<typename L>
    friend void detail::adopt_peer(L &, ::asio::local::stream_protocol::socket);
    template<typename L>
    friend void detail::do_accept(L &);

    ::asio::io_context &m_io;
    ::asio::local::stream_protocol::acceptor m_acceptor;
    stream::stream_inbound_config m_cfg;
    plexus::detail::socket_mode m_mode;
    const io::security::peer_cred_policy *m_peer_policy; // borrowed; never owned
    io::congestion m_congestion;
    io::egress_capacity m_egress_capacity;
    stream_socket_options m_socket_options;
    std::string m_bound_path;
    plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> m_on_accepted_cb;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    bool m_running{false};
};

}

#endif

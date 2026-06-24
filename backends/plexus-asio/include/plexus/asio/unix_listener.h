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

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <sys/stat.h>
#include <unistd.h>
#if defined(__linux__)
    #include <sys/socket.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
    #include <unistd.h>
#endif

#include <string>
#include <memory>
#include <cerrno>
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
    static constexpr ::mode_t default_socket_mode = S_IRWXU;

    explicit unix_listener(::asio::io_context &io, stream::stream_inbound_config cfg = {}, ::mode_t mode = default_socket_mode,
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
#if !defined(__linux__) && !defined(__APPLE__) && !defined(__FreeBSD__)
        // Windows AF_UNIX exposes no peer credentials. A non-accept_any policy cannot be
        // honored, so refuse at listen (fail-closed) rather than silently admit every peer.
        if(!m_peer_policy->accepts_without_credentials())
            return detail::report(*this, std::make_error_code(std::errc::operation_not_supported));
#endif
        const auto &path = bind_ep.address;
        detail::unlink_path(path); // clear a stale socket file before bind
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return detail::report(*this, ec);
        // Bind under a restrictive umask so the socket inode is created with NO access for
        // group/other from the outset — this closes the TOCTOU window between bind (which
        // would otherwise create a world-accessible inode) and the mode application. The
        // umask is restored immediately after bind.
        const ::mode_t prev_umask = ::umask(0077);
        m_acceptor.bind(ep, ec); // NO reuse_address for AF_UNIX
        (void)::umask(prev_umask);
        if(ec)
            return detail::abort_start(*this, ec); // close the opened acceptor
        m_bound_path = path;                       // bind created the inode — own it so any later failure unlinks it
        if(::chmod(path.c_str(), m_mode) != 0)     // apply the configured mode (0700 default, or a widened knob)
            return detail::abort_start(*this, std::error_code(errno, std::generic_category()));
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
            detail::unlink_path(m_bound_path);
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
    ::mode_t m_mode;
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

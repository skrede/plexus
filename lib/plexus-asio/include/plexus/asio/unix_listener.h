#ifndef HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H
#define HPP_GUARD_PLEXUS_ASIO_UNIX_LISTENER_H

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/detail/asio_error_map.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/wire/stream_inbound.h"

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

// AF_UNIX acceptor over ::asio::local::stream_protocol::acceptor. A protocol-type
// swap of asio_listener plus the socket-file lifecycle — the only genuinely new
// mechanics here. start(endpoint) parses the path, unlinks a STALE socket file
// left by a crashed prior run (so bind cannot wedge with EADDRINUSE), opens, binds,
// chmods the socket owner-only (0700), and listens, then loops accepting; each
// accepted socket is adopted by a fresh unix_channel (already reading) and handed
// to on_accepted as a unique_ptr owner. stop() removes the filesystem entry the
// acceptor close leaves behind, but only a path THIS listener bound. There is NO
// reuse_address (AF_UNIX has none) and NO port (the rendezvous is the path).
//
// SECURITY CONTRACT (caller-owned): the socket path MUST live in a directory the
// owner controls (not a world-writable shared tmp), so the unlink-before-bind cannot
// be raced. The socket mode (0700 owner-only by default, a widened knob) is the access
// boundary; the bind happens under a restrictive umask so the inode is created at the
// mode atomically (no post-bind chmod window). An injected peer_cred_policy is
// defense-in-depth ABOVE the mode: it judges {uid, gid, pid} at accept and refuses an
// unauthorized local peer fail-closed (accept_any is the named default).
class unix_listener
{
public:
    // The default socket mode: owner-only 0700, the fail-closed boundary. The knob widens
    // deliberately (e.g. 0770) — loosening is an informed choice, never a default.
    static constexpr ::mode_t default_socket_mode = S_IRWXU;

    // cfg is the node-level hardening config (required-WITH-default), stamped onto every
    // channel this listener accepts. mode is the socket-file permission (required-WITH-
    // default 0700). policy is the borrowed accept-time peer-credential allowlist
    // (defense-in-depth; the named accept_any default admits every local peer).
    explicit unix_listener(::asio::io_context &io, wire::stream_inbound_config cfg = {},
                           ::mode_t                              mode = default_socket_mode,
                           const io::security::peer_cred_policy &policy =
                                   io::security::shared_accept_any_peer_cred(),
                           io::congestion        congestion = io::congestion::block,
                           io::egress_capacity   egress = io::egress_capacity::bounded_default(),
                           stream_socket_options socket_options = {})
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
        m_on_accepted = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    ~unix_listener() { stop(); }

    void start(const io::endpoint &bind_ep)
    {
        std::error_code ec;
        auto            ep = detail::parse_unix(bind_ep.address, ec);
        if(ec)
            return report(ec);
#if !defined(__linux__) && !defined(__APPLE__) && !defined(__FreeBSD__)
        // Windows AF_UNIX exposes no peer credentials. A non-accept_any policy cannot be
        // honored, so refuse at listen (fail-closed) rather than silently admit every peer.
        if(!m_peer_policy->accepts_without_credentials())
            return report(std::make_error_code(std::errc::operation_not_supported));
#endif
        const auto &path = bind_ep.address;
        unlink_path(path); // clear a stale socket file before bind
        m_acceptor.open(ep.protocol(), ec);
        if(ec)
            return report(ec);
        // Bind under a restrictive umask so the socket inode is created with NO access for
        // group/other from the outset — this closes the TOCTOU window between bind (which
        // would otherwise create a world-accessible inode) and the mode application. The
        // umask is restored immediately after bind.
        const ::mode_t prev_umask = ::umask(0077);
        m_acceptor.bind(ep, ec); // NO reuse_address for AF_UNIX
        (void)::umask(prev_umask);
        if(ec)
            return abort_start(ec); // close the opened acceptor
        m_bound_path = path; // bind created the inode — own it so any later failure unlinks it
        if(::chmod(path.c_str(), m_mode) !=
           0) // apply the configured mode (0700 default, or a widened knob)
            return abort_start(std::error_code(errno, std::generic_category()));
        m_acceptor.listen(::asio::socket_base::max_listen_connections, ec);
        if(ec)
            return abort_start(ec);
        m_running = true;
        do_accept();
    }

    void stop()
    {
        m_running = false;
        std::error_code ec;
        (void)m_acceptor.cancel(ec);
        (void)m_acceptor.close(ec);
        if(!m_bound_path.empty()) // only unlink a path THIS listener bound
        {
            unlink_path(m_bound_path);
            m_bound_path.clear();
        }
    }

private:
    // Unwind a partially-started listener: close the opened acceptor and unlink the
    // socket file bind() already created, so a failed start leaks neither the open
    // acceptor (a retry would wedge at already_open) nor the on-disk inode.
    void abort_start(const std::error_code &ec)
    {
        std::error_code ignore;
        (void)m_acceptor.close(ignore);
        if(!m_bound_path.empty())
        {
            unlink_path(m_bound_path);
            m_bound_path.clear();
        }
        report(ec);
    }

    // Best-effort remove of a socket file: ENOENT (nothing there) is success, and any
    // real conflict surfaces at the subsequent bind rather than here.
    static void unlink_path(const std::string &path) { (void)::unlink(path.c_str()); }

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
                    // Consult the borrowed peer-credential policy BEFORE adopting the channel.
                    // A reject closes the accepted socket fail-closed (no channel handed up),
                    // the session stays unestablished, and the accept loop continues.
                    if(!admit_peer(peer))
                    {
                        std::error_code ic;
                        (void)peer.close(ic);
                    }
                    else
                    {
                        auto channel = std::make_unique<unix_channel>(
                                m_io, std::move(peer), m_cfg, m_congestion, m_egress_capacity,
                                m_socket_options);
                        if(m_on_accepted)
                            m_on_accepted(std::move(channel));
                    }
                    if(m_running)
                        do_accept();
                });
    }

    // Read the accepted peer's local credentials and run them past the injected policy. The
    // credential read is per-platform (Linux SO_PEERCRED, macOS/BSD getpeereid with pid=0).
    // A read failure is itself a refusal (fail-closed — an unidentifiable peer is not
    // admitted). NOTE: the macOS/BSD path is unverified on this Linux host (flagged for CI).
    bool admit_peer(::asio::local::stream_protocol::socket &peer) const
    {
#if defined(__linux__)
        ::ucred     cred{};
        ::socklen_t len = sizeof(cred);
        if(::getsockopt(peer.native_handle(), SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0)
            return false;
        return m_peer_policy->decide(io::security::peer_cred{static_cast<std::uint32_t>(cred.uid),
                                                             static_cast<std::uint32_t>(cred.gid),
                                                             static_cast<std::uint32_t>(cred.pid)});
#elif defined(__APPLE__) || defined(__FreeBSD__)
        ::uid_t uid = 0;
        ::gid_t gid = 0;
        if(::getpeereid(peer.native_handle(), &uid, &gid) != 0) // no pid on Darwin/BSD => pid=0
            return false;
        return m_peer_policy->decide(io::security::peer_cred{static_cast<std::uint32_t>(uid),
                                                             static_cast<std::uint32_t>(gid), 0});
#else
        (void)peer;
        return m_peer_policy
                ->accepts_without_credentials(); // Windows AF_UNIX: no creds (refused at listen)
#endif
    }

    void report(const std::error_code &ec)
    {
        if(m_on_error)
            m_on_error(detail::map_error(ec));
    }

    ::asio::io_context                      &m_io;
    ::asio::local::stream_protocol::acceptor m_acceptor;
    wire::stream_inbound_config              m_cfg;
    ::mode_t                                 m_mode;
    const io::security::peer_cred_policy    *m_peer_policy; // borrowed; never owned
    io::congestion                           m_congestion;
    io::egress_capacity                      m_egress_capacity;
    stream_socket_options                    m_socket_options;
    std::string                              m_bound_path;
    plexus::detail::move_only_function<void(std::unique_ptr<unix_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(io::io_error)>                  m_on_error;
    bool                                                                    m_running{false};
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_ACCEPT_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_ACCEPT_H

#include "plexus/asio/unix_channel.h"

#include "plexus/io/io_error.h"
#include "plexus/io/security/peer_cred_policy.h"

#include <asio/local/stream_protocol.hpp>

#include <memory>
#include <string>
#include <cstdint>
#include <utility>
#include <system_error>

#include <unistd.h>
#include <sys/socket.h>

namespace plexus::asio::detail {

template<typename L>
void report(L &l, const std::error_code &ec)
{
    if(l.m_on_error)
        l.m_on_error(detail::map_error(ec));
}

// ENOENT is success; any real conflict surfaces at the subsequent bind rather than here.
inline void unlink_path(const std::string &path)
{
    (void)::unlink(path.c_str());
}

// Close the acceptor and unlink the socket file bind() created so a failed start leaks neither the
// open acceptor nor the on-disk inode.
template<typename L>
void abort_start(L &l, const std::error_code &ec)
{
    std::error_code ignore;
    (void)l.m_acceptor.close(ignore);
    if(!l.m_bound_path.empty())
    {
        unlink_path(l.m_bound_path);
        l.m_bound_path.clear();
    }
    report(l, ec);
}

// A credential read failure is itself a refusal (fail-closed). The macOS/BSD getpeereid path is
// unverified on this Linux host.
template<typename L>
bool admit_peer(const L &l, ::asio::local::stream_protocol::socket &peer)
{
#if defined(__linux__)
    ::ucred cred{};
    ::socklen_t len = sizeof(cred);
    if(::getsockopt(peer.native_handle(), SOL_SOCKET, SO_PEERCRED, &cred, &len) != 0)
        return false;
    return l.m_peer_policy->decide(io::security::peer_cred{static_cast<std::uint32_t>(cred.uid), static_cast<std::uint32_t>(cred.gid), static_cast<std::uint32_t>(cred.pid)});
#elif defined(__APPLE__) || defined(__FreeBSD__)
    ::uid_t uid = 0;
    ::gid_t gid = 0;
    if(::getpeereid(peer.native_handle(), &uid, &gid) != 0) // no pid on Darwin/BSD => pid=0
        return false;
    return l.m_peer_policy->decide(io::security::peer_cred{static_cast<std::uint32_t>(uid), static_cast<std::uint32_t>(gid), 0});
#else
    (void)peer;
    return l.m_peer_policy->accepts_without_credentials();
#endif
}

// A rejected peer's socket is closed (fail-closed, no channel handed up).
template<typename L>
void adopt_peer(L &l, ::asio::local::stream_protocol::socket peer)
{
    if(!admit_peer(l, peer))
    {
        std::error_code ic;
        (void)peer.close(ic);
        return;
    }
    auto channel = std::make_unique<unix_channel>(l.m_io, std::move(peer), l.m_cfg, l.m_congestion, l.m_egress_capacity, l.m_socket_options);
    if(l.m_on_accepted)
        l.m_on_accepted(std::move(channel));
}

template<typename L>
void do_accept(L &l)
{
    l.m_acceptor.async_accept(
            [&l](std::error_code ec, ::asio::local::stream_protocol::socket peer)
            {
                if(ec)
                {
                    if(ec != ::asio::error::operation_aborted)
                        report(l, ec);
                    return;
                }
                adopt_peer(l, std::move(peer));
                if(l.m_running)
                    do_accept(l);
            });
}

}

#endif

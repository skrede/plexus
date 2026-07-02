#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_ACCEPT_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_ACCEPT_H

#include "plexus/asio/unix_channel.h"

#include "plexus/io/io_error.h"
#include "plexus/io/security/peer_cred_policy.h"
#include "plexus/detail/socket_compat.h"

#include <asio/local/stream_protocol.hpp>

#include <memory>
#include <string>
#include <utility>
#include <system_error>

namespace plexus::asio::detail {

template<typename L>
void report(L &l, const std::error_code &ec)
{
    if(l.m_on_error_cb)
        l.m_on_error_cb(detail::map_error(ec));
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
        plexus::detail::remove_socket_path(l.m_bound_path);
        l.m_bound_path.clear();
    }
    report(l, ec);
}

// A credential read failure is itself a refusal (fail-closed); a platform without peer creds defers
// to the policy's accept-without-credentials verdict instead.
template<typename L>
bool admit_peer(const L &l, ::asio::local::stream_protocol::socket &peer)
{
    if constexpr(plexus::detail::peer_cred_supported)
    {
        io::security::peer_cred cred;
        if(!plexus::detail::read_peer_cred(peer.native_handle(), cred))
            return false;
        return l.m_peer_policy->decide(cred);
    }
    else
    {
        (void)peer;
        return l.m_peer_policy->accepts_without_credentials();
    }
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
    if(l.m_on_accepted_cb)
        l.m_on_accepted_cb(std::move(channel));
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

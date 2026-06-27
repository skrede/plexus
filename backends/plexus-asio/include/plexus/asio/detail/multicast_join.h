#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_MULTICAST_JOIN_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_MULTICAST_JOIN_H

#include <asio/ip/udp.hpp>
#include <asio/ip/multicast.hpp>
#include <asio/ip/address_v4.hpp>

#include <cstdint>
#include <system_error>

namespace plexus::asio::detail {

// The one new mechanism over udp_server: bind to ANY:port (so every interface's inbound on the
// group is delivered), issue the IPv4 IGMP join, scope the egress by TTL, and enable loopback so a
// co-resident two-node delivery is observable. reuse_address lets co-resident joiners share the
// port. Fail-closed: the first error short-circuits via ec and the caller never arms the recv loop.
inline void join_multicast_group(::asio::ip::udp::socket &socket, const ::asio::ip::address_v4 &group, std::uint16_t port, unsigned ttl, std::error_code &ec)
{
    socket.set_option(::asio::socket_base::reuse_address(true), ec);
    if(ec)
        return;
    socket.bind({::asio::ip::udp::endpoint{::asio::ip::address_v4::any(), port}}, ec);
    if(ec)
        return;
    socket.set_option(::asio::ip::multicast::join_group(group), ec);
    if(ec)
        return;
    socket.set_option(::asio::ip::multicast::hops(static_cast<int>(ttl)), ec);
    if(ec)
        return;
    socket.set_option(::asio::ip::multicast::enable_loopback(true), ec);
}

}

#endif

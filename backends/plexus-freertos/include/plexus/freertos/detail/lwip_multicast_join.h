#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_MULTICAST_JOIN_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_MULTICAST_JOIN_H

// The one new mechanism over the lwIP TCP leaf: the IPv4 IGMP join. It exists ONLY on-target — the
// host build reaches none of it (the host's asio sibling carries its own join). Mirrors the asio
// detail/multicast_join.h sequence and ADDS the egress-interface selection (IP_MULTICAST_IF) the
// asio path does not need: the ESP32 STA netif must be named explicitly or the device's own
// announcements never leave the right interface.

#if defined(ESP_PLATFORM)

    #include "lwip/sockets.h"

    #include <cerrno>
    #include <cstdint>
    #include <system_error>

namespace plexus::freertos::detail {

// Apply one socket option, fail-closed: the first nonzero return latches ec from errno so the caller
// never arms the receive on a half-configured socket.
inline void set_or_fail(int fd, int level, int name, const void *val, std::uint32_t len, std::error_code &ec)
{
    if(ec)
        return;
    if(lwip_setsockopt(fd, level, name, val, len) < 0)
        ec = std::error_code{errno, std::generic_category()};
}

// Bind to ANY:port (so every interface's inbound on the group is delivered), join the group, select
// the STA egress interface (the lwIP-specific step, set from the DHCP-assigned address passed in,
// never baked), scope the egress by ttl, and enable loopback. Source: the shipped esp-idf
// udp_multicast example's setsockopt sequence.
inline void join_multicast_group(int fd, in_addr_t group, in_addr_t egress, std::uint16_t port, std::uint8_t ttl, std::error_code &ec)
{
    sockaddr_in bind_addr{};
    bind_addr.sin_family      = AF_INET;
    bind_addr.sin_port        = lwip_htons(port);
    bind_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if(lwip_bind(fd, reinterpret_cast<sockaddr *>(&bind_addr), sizeof(bind_addr)) < 0)
        ec = std::error_code{errno, std::generic_category()};

    ip_mreq mreq{};
    mreq.imr_multiaddr.s_addr = group;
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    set_or_fail(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq), ec);
    in_addr egress_if{egress};
    set_or_fail(fd, IPPROTO_IP, IP_MULTICAST_IF, &egress_if, sizeof(egress_if), ec);
    set_or_fail(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl), ec);
    const std::uint8_t loop = 1;
    set_or_fail(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop), ec);
}

}

#endif

#endif

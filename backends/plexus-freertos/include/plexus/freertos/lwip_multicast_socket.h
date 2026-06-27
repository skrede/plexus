#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_MULTICAST_SOCKET_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_MULTICAST_SOCKET_H

#include "plexus/freertos/detail/lwip_endpoint.h"
#include "plexus/freertos/detail/lwip_multicast_join.h"

#include "plexus/stream/datagram_socket.h"
#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <string>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <system_error>

#if defined(ESP_PLATFORM)
    #include "lwip/sockets.h"
    #include "lwip/ip4_addr.h"
    #include <cerrno>
#endif

namespace plexus::freertos {

class freertos_executor;

// The on-target counterpart of the asio udp_multicast_socket: the same member surface
// (bind/send_multicast/on_datagram/close + endpoint_type) so the hoisted multicast_discovery binds
// to it verbatim. It names the lwIP BSD socket calls, isolates the IGMP join in a detail unit, and
// drains inbound in the executor's poll() step (P1) rather than a dedicated RX task — discovery is
// sparse small datagrams and an extra thread is INV friction. The leaf body exists ONLY on-target;
// the host build reaches the endpoint wrapper alone (its seam TU proves the endpoint contract).
class lwip_multicast_socket
{
public:
    using endpoint_type = detail::lwip_endpoint;

    lwip_multicast_socket(freertos_executor &executor, std::string group, std::uint16_t port, std::uint8_t ttl, std::string egress)
            : m_executor(executor)
            , m_group(std::move(group))
            , m_egress(std::move(egress))
            , m_recv_buf{}
            , m_port(port)
            , m_ttl(ttl)
            , m_fd(-1)
            , m_open(false)
    {
    }

    lwip_multicast_socket(const lwip_multicast_socket &)            = delete;
    lwip_multicast_socket &operator=(const lwip_multicast_socket &) = delete;

#if defined(ESP_PLATFORM)
    ~lwip_multicast_socket()
    {
        close();
    }

    // Fail-closed: an open/join error leaves the receive unarmed, mirroring the asio sibling.
    std::error_code bind(const endpoint_type &)
    {
        m_fd = lwip_socket(AF_INET, SOCK_DGRAM, IPPROTO_IP);
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        set_nonblocking();
        std::error_code ec;
        detail::join_multicast_group(m_fd, inet_addr(m_group.c_str()), inet_addr(m_egress.c_str()), m_port, m_ttl, ec);
        if(ec)
            return close(), ec;
        m_open = true;
        return ec;
    }

    void send_multicast(std::span<const std::byte> bytes)
    {
        if(!m_open)
            return;
        sockaddr_in to{};
        to.sin_family      = AF_INET;
        to.sin_port        = lwip_htons(m_port);
        to.sin_addr.s_addr = inet_addr(m_group.c_str());
        lwip_sendto(m_fd, bytes.data(), bytes.size(), 0, reinterpret_cast<sockaddr *>(&to), sizeof(to));
    }

    // The P1 drain the super-loop calls each poll() pass: take one inbound, extract the unspoofable
    // source as a bare dotted-quad from the kernel sockaddr_in, and fire the callback. A fixed recv
    // slot bounds memory — a large/garbage datagram never grows the heap.
    void poll()
    {
        if(!m_open || !m_on_datagram_cb)
            return;
        sockaddr_in from{};
        socklen_t   from_len = sizeof(from);
        const int   n = lwip_recvfrom(m_fd, m_recv_buf.data(), m_recv_buf.size(), MSG_DONTWAIT, reinterpret_cast<sockaddr *>(&from), &from_len);
        if(n <= 0)
            return;
        m_on_datagram_cb(source_endpoint(from), {m_recv_buf.data(), static_cast<std::size_t>(n)});
    }

    void close()
    {
        if(m_fd >= 0)
            lwip_close(m_fd);
        m_fd   = -1;
        m_open = false;
    }
#endif

    void on_datagram(::plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram_cb = std::move(cb);
    }

private:
#if defined(ESP_PLATFORM)
    void set_nonblocking()
    {
        const int flags = lwip_fcntl(m_fd, F_GETFL, 0);
        lwip_fcntl(m_fd, F_SETFL, flags | O_NONBLOCK);
    }

    static endpoint_type source_endpoint(const sockaddr_in &from)
    {
        ip4_addr_t addr{};
        addr.addr = from.sin_addr.s_addr;
        char dotted[IP4ADDR_STRLEN_MAX]{};
        ip4addr_ntoa_r(&addr, dotted, sizeof(dotted));
        return endpoint_type{std::string{dotted}};
    }
#endif

    freertos_executor                                                                                       &m_executor;
    std::string                                                                                              m_group;
    std::string                                                                                              m_egress;
    std::array<std::byte, 1500>                                                                              m_recv_buf;
    ::plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram_cb;
    std::uint16_t                                                                                            m_port;
    std::uint8_t                                                                                             m_ttl;
    int                                                                                                      m_fd;
    bool                                                                                                     m_open;
};

#if defined(ESP_PLATFORM)
static_assert(stream::datagram_socket<lwip_multicast_socket>);
#endif

}

#endif

#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ACCEPTOR_IO_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ACCEPTOR_IO_H

// The server-side sibling of lwip_socket_io.h: the one acceptor that names the lwIP BSD listen()/
// accept() surface. Like the connected socket, it exists ONLY on-target — the host suite binds a
// genuine POSIX listener test-side (host_tcp_listener.h), so nothing here reaches the PC build.

#if defined(ESP_PLATFORM)

    #include "plexus/freertos/detail/lwip_socket_io.h"

    #include "plexus/io/endpoint.h"

    #include "lwip/sockets.h"

    #include <cerrno>
    #include <optional>
    #include <system_error>

namespace plexus::freertos::detail {

// The on-target listening socket. bind_and_listen() opens a non-blocking listener; accept_one()
// returns an lwip_socket adopting an accepted fd, or nullopt when nothing is pending (the
// non-blocking case the transport's poll() drives once per super-loop step). OS-mechanism only —
// the channel-minting and view-tracking are transport-side.
class lwip_acceptor
{
public:
    lwip_acceptor()
            : m_fd(-1)
    {
    }

    ~lwip_acceptor()
    {
        close();
    }

    lwip_acceptor(const lwip_acceptor &)            = delete;
    lwip_acceptor &operator=(const lwip_acceptor &) = delete;

    std::error_code bind_and_listen(plexus::io::endpoint ep)
    {
        const auto parsed = parse_host_port(ep.address);
        m_fd              = ::lwip_socket(AF_INET, SOCK_STREAM, 0); // the BSD opener; the class name shadows it unqualified here
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = lwip_htons(parsed.port);
        if(lwip_bind(m_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0 || lwip_listen(m_fd, k_backlog) < 0)
            return std::make_error_code(std::errc::address_in_use);
        set_nonblocking();
        return {};
    }

    std::optional<lwip_socket> accept_one()
    {
        const int fd = lwip_accept(m_fd, nullptr, nullptr);
        if(fd < 0)
            return std::nullopt;
        return lwip_socket{fd};
    }

    void close()
    {
        if(m_fd >= 0)
            lwip_close(m_fd);
        m_fd = -1;
    }

private:
    void set_nonblocking()
    {
        const int flags = lwip_fcntl(m_fd, F_GETFL, 0);
        lwip_fcntl(m_fd, F_SETFL, flags | O_NONBLOCK);
    }

    static constexpr int k_backlog = 4;

    int m_fd;
};

}

#endif

#endif

#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_LISTENER_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_LISTENER_H

// The test-side acceptor: a GENUINE POSIX listener over loopback, mirroring the seam of the
// on-target lwIP acceptor (bind_and_listen / accept_one / close) so the transport templates over
// either. Test-local — never shipped in lib (on-target the lwIP acceptor in
// detail/lwip_acceptor_io.h plays this role).
#if !defined(ESP_PLATFORM)

    #include "host_tcp_socket.h"

    #include "plexus/io/endpoint.h"

    #include <fcntl.h>
    #include <netinet/in.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <unistd.h>

    #include <string>
    #include <cstdint>
    #include <optional>
    #include <system_error>

namespace plexus::test {

// A non-blocking POSIX listener. bind_and_listen() binds the requested port (0 => an ephemeral
// port, recovered through local_endpoint()); accept_one() yields a host_tcp_socket adopting the
// accepted fd, or nullopt when nothing is pending.
class host_tcp_listener
{
public:
    host_tcp_listener()
            : m_fd(-1)
            , m_port(0)
    {
    }

    ~host_tcp_listener()
    {
        close();
    }

    host_tcp_listener(const host_tcp_listener &)            = delete;
    host_tcp_listener &operator=(const host_tcp_listener &) = delete;

    std::error_code bind_and_listen(plexus::io::endpoint ep)
    {
        const auto [host, port] = split_host_port(ep.address);
        m_fd                    = ::socket(AF_INET, SOCK_STREAM, 0);
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port);
        socklen_t len        = sizeof(addr);
        if(::bind(m_fd, reinterpret_cast<sockaddr *>(&addr), len) < 0 || ::listen(m_fd, 4) < 0 || ::getsockname(m_fd, reinterpret_cast<sockaddr *>(&addr), &len) < 0)
            return std::make_error_code(std::errc::address_in_use);
        m_port = ntohs(addr.sin_port);
        ::fcntl(m_fd, F_SETFL, ::fcntl(m_fd, F_GETFL, 0) | O_NONBLOCK);
        return {};
    }

    std::optional<host_tcp_socket> accept_one()
    {
        const int fd = ::accept(m_fd, nullptr, nullptr);
        if(fd < 0)
            return std::nullopt;
        return host_tcp_socket{fd};
    }

    plexus::io::endpoint local_endpoint() const
    {
        return plexus::io::endpoint{"tcp", "127.0.0.1:" + std::to_string(m_port)};
    }

    void close()
    {
        if(m_fd >= 0)
            ::close(m_fd);
        m_fd = -1;
    }

private:
    int           m_fd;
    std::uint16_t m_port;
};

}

#endif

#endif

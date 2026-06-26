#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H

// A GENUINE POSIX-TCP stream_socket the host slice connects over loopback — NOT a mock and NOT a
// synthetic byte buffer. It exercises the channel's framing against real partial reads and real
// EWOULDBLOCK on a non-blocking 127.0.0.1 socket, so the loopback round-trip proves the dial path,
// the stream_inbound reassembly, and the P1 poll-drive end-to-end with no hardware. Test-local —
// never shipped in lib (on-target the lwIP socket in detail/lwip_socket_io.h plays this role).
#if !defined(ESP_PLATFORM)

    #include "plexus/io/endpoint.h"

    #include <fcntl.h>
    #include <netinet/in.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <unistd.h>

    #include <span>
    #include <cerrno>
    #include <string>
    #include <utility>
    #include <cstddef>
    #include <cstdint>
    #include <charconv>
    #include <optional>
    #include <string_view>
    #include <system_error>

namespace plexus::test {

inline std::pair<std::string, std::uint16_t> split_host_port(std::string_view address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string_view::npos)
        return {std::string{address}, 0};
    std::uint16_t port = 0;
    const auto tail    = address.substr(colon + 1);
    std::from_chars(tail.data(), tail.data() + tail.size(), port);
    return {std::string{address.substr(0, colon)}, port};
}

// A non-blocking connected TCP socket over loopback. A recv() of 0 with no transient errno means an
// orderly peer FIN (a hard drop the channel turns into on_error); EWOULDBLOCK is "nothing yet".
class host_tcp_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    host_tcp_socket()
            : m_fd(-1)
            , m_closed(false)
    {
    }

    explicit host_tcp_socket(int fd)
            : m_fd(fd)
            , m_closed(false)
    {
        set_nonblocking();
    }

    ~host_tcp_socket()
    {
        close();
    }

    host_tcp_socket(const host_tcp_socket &)            = delete;
    host_tcp_socket &operator=(const host_tcp_socket &) = delete;

    host_tcp_socket(host_tcp_socket &&other) noexcept
            : m_fd(other.m_fd)
            , m_closed(other.m_closed)
    {
        other.m_fd     = -1;
        other.m_closed = true;
    }

    host_tcp_socket &operator=(host_tcp_socket &&other) noexcept
    {
        if(this != &other)
        {
            close();
            m_fd           = other.m_fd;
            m_closed       = other.m_closed;
            other.m_fd     = -1;
            other.m_closed = true;
        }
        return *this;
    }

    std::error_code connect(endpoint_type ep)
    {
        const auto [host, port] = split_host_port(ep.address);
        m_fd                    = ::socket(AF_INET, SOCK_STREAM, 0);
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        if(::connect(m_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
            return std::make_error_code(std::errc::connection_refused);
        set_nonblocking();
        m_closed = false;
        return {};
    }

    std::size_t send(std::span<const std::byte> bytes)
    {
        const auto n = ::send(m_fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        return classify_io(static_cast<int>(n));
    }

    std::size_t recv(std::span<std::byte> buf)
    {
        const auto n = ::recv(m_fd, buf.data(), buf.size(), 0);
        if(n == 0)
            m_closed = true;
        return classify_io(static_cast<int>(n));
    }

    bool closed() const
    {
        return m_closed;
    }

    void close()
    {
        if(m_fd >= 0)
            ::close(m_fd);
        m_fd     = -1;
        m_closed = true;
    }

private:
    void set_nonblocking()
    {
        const int flags = ::fcntl(m_fd, F_GETFL, 0);
        ::fcntl(m_fd, F_SETFL, flags | O_NONBLOCK);
    }

    std::size_t classify_io(int n)
    {
        if(n > 0)
            return static_cast<std::size_t>(n);
        if(n < 0 && (errno == ECONNRESET || errno == EPIPE))
            m_closed = true;
        return 0;
    }

    int  m_fd;
    bool m_closed;
};

// A connected loopback pair: binds a transient listener on 127.0.0.1:0, dials it, and returns both
// halves so a test drives two channels over one real TCP connection. The accepted end is yielded as
// a host_tcp_socket adopting the accepted fd (the transport's dial-only half never accepts).
struct loopback_pair
{
    host_tcp_socket dialed;
    host_tcp_socket accepted;
    std::string     dial_address;
};

inline std::optional<loopback_pair> make_loopback_pair()
{
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if(listener < 0)
        return std::nullopt;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    socklen_t len        = sizeof(addr);
    if(::bind(listener, reinterpret_cast<sockaddr *>(&addr), len) < 0 || ::listen(listener, 1) < 0 || ::getsockname(listener, reinterpret_cast<sockaddr *>(&addr), &len) < 0)
    {
        ::close(listener);
        return std::nullopt;
    }
    const std::string address = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));
    host_tcp_socket dialed;
    if(dialed.connect(plexus::io::endpoint{"tcp", address}))
    {
        ::close(listener);
        return std::nullopt;
    }
    const int accepted_fd = ::accept(listener, nullptr, nullptr);
    ::close(listener);
    if(accepted_fd < 0)
        return std::nullopt;
    return loopback_pair{std::move(dialed), host_tcp_socket{accepted_fd}, address};
}

}

#endif

#endif

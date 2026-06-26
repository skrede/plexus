#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_IO_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_IO_H

// The asio->lwIP transport leg: the one stream_socket implementor that names the lwIP BSD
// socket surface. It exists ONLY on-target — the host suite binds a genuine POSIX-TCP
// stream_socket declared test-side (a real working socket, not a shim), so nothing in this
// header reaches the PC build. Mirrors how detail/uart_io.h gates the ESP-IDF UART driver.

#if defined(ESP_PLATFORM)

    #include "plexus/io/endpoint.h"

    #include "lwip/sockets.h"
    #include "lwip/netdb.h"

    #include <span>
    #include <cerrno>
    #include <string>
    #include <cstddef>
    #include <charconv>
    #include <string_view>
    #include <system_error>

namespace plexus::freertos::detail {

struct lwip_address
{
    std::string host;
    std::uint16_t port;
};

// Split a "host:port" address (the io::endpoint::address form) into its two parts; an absent
// or unparsable port yields port 0, which connect() then rejects.
inline lwip_address parse_host_port(std::string_view address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string_view::npos)
        return {std::string{address}, 0};
    std::uint16_t port = 0;
    const auto tail = address.substr(colon + 1);
    std::from_chars(tail.data(), tail.data() + tail.size(), port);
    return {std::string{address.substr(0, colon)}, port};
}

// The on-target connected TCP socket. Non-blocking throughout so the channel's poll() drains
// recv() inside the cooperative loop and send() never parks it.
class lwip_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    lwip_socket()
            : m_fd(-1)
            , m_closed(false)
    {
    }

    // Adopt an accepted fd (the acceptor leaf's accept() result); set non-blocking so the channel's
    // poll() drains recv() cooperatively, mirroring the dialed socket's set_nonblocking().
    explicit lwip_socket(int fd)
            : m_fd(fd)
            , m_closed(false)
    {
        set_nonblocking();
    }

    ~lwip_socket()
    {
        close();
    }

    lwip_socket(const lwip_socket &)            = delete;
    lwip_socket &operator=(const lwip_socket &) = delete;

    lwip_socket(lwip_socket &&other) noexcept
            : m_fd(other.m_fd)
            , m_closed(other.m_closed)
    {
        other.m_fd     = -1;
        other.m_closed = true;
    }

    lwip_socket &operator=(lwip_socket &&other) noexcept
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
        const auto parsed = parse_host_port(ep.address);
        if(std::error_code ec = open_and_dial(parsed); ec)
            return ec;
        m_closed = false;
        return {};
    }

    std::size_t send(std::span<const std::byte> bytes)
    {
        const int n = lwip_send(m_fd, bytes.data(), bytes.size(), MSG_DONTWAIT);
        return classify_io(n);
    }

    std::size_t recv(std::span<std::byte> buf)
    {
        const int n = lwip_recv(m_fd, buf.data(), buf.size(), MSG_DONTWAIT);
        if(n == 0) // an orderly peer FIN is a hard drop
            m_closed = true;
        return classify_io(n);
    }

    bool closed() const
    {
        return m_closed;
    }

    void close()
    {
        if(m_fd >= 0)
            lwip_close(m_fd);
        m_fd     = -1;
        m_closed = true;
    }

    // The RX-task receive policy parks in a blocking recv instead of spinning a non-blocking poll;
    // toggling the socket blocking is the P2 opt-in (P1 stays non-blocking for its cooperative drain).
    void set_blocking(bool blocking)
    {
        const int flags = lwip_fcntl(m_fd, F_GETFL, 0);
        lwip_fcntl(m_fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
    }

private:
    std::error_code open_and_dial(const lwip_address &parsed)
    {
        m_fd = ::lwip_socket(AF_INET, SOCK_STREAM, 0); // the BSD opener; the class name shadows it unqualified here
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = lwip_htons(parsed.port);
        inet_pton(AF_INET, parsed.host.c_str(), &addr.sin_addr);
        const int rc = lwip_connect(m_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
        if(rc < 0 && errno != EINPROGRESS)
            return std::make_error_code(std::errc::connection_refused);
        set_nonblocking();
        return {};
    }

    void set_nonblocking()
    {
        const int flags = lwip_fcntl(m_fd, F_GETFL, 0);
        lwip_fcntl(m_fd, F_SETFL, flags | O_NONBLOCK);
    }

    // A negative return splits into two classes kept textually distinct (research §5). SOFT/transient
    // — EWOULDBLOCK/EAGAIN (send buffer full) or ENOMEM (lwIP ERR_MEM: a tcp_write/pbuf OOM) — is local
    // congestion: report 0, the channel re-arms, it NEVER tears down. HARD — ECONNRESET/EPIPE — is a
    // connection drop: set closed so the channel fires on_error and the engine re-dials.
    std::size_t classify_io(int n)
    {
        if(n > 0)
            return static_cast<std::size_t>(n);
        const bool soft = errno == EWOULDBLOCK || errno == EAGAIN || errno == ENOMEM;
        if(n < 0 && !soft && (errno == ECONNRESET || errno == EPIPE))
            m_closed = true;
        return 0;
    }

    int  m_fd;
    bool m_closed;
};

}

#endif

#endif

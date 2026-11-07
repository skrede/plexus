#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_IO_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_IO_H

// The asio->lwIP transport leg: the one stream_socket implementor that names the lwIP BSD
// socket surface. It exists ONLY on-target — the host suite binds a genuine POSIX-TCP
// stream_socket declared test-side (a real working socket, not a shim), so nothing in this
// header reaches the PC build. Mirrors how detail/uart_io.h gates the ESP-IDF UART driver.

#if defined(ESP_PLATFORM)

    #include "plexus/freertos/detail/lwip_socket_ops.h"

    #include "plexus/io/endpoint.h"

    #include "lwip/sockets.h"
    #include "lwip/netdb.h"

    #include <span>
    #include <atomic>
    #include <cerrno>
    #include <chrono>
    #include <cstddef>
    #include <optional>
    #include <system_error>

namespace plexus::freertos::detail {

// The on-target TCP socket. connect() starts a non-blocking connect that connect_outcome() resolves; the socket then
// receives in the mode the policy picks: non-blocking for the channel's cooperative poll(), blocking for the receive
// task, whose recv() parks for the receive timeout. send() never parks.
class lwip_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    lwip_socket()
            : m_fd(-1)
            , m_closed(false)
            , m_blocking(false)
            , m_connect_outcome(std::nullopt)
    {
    }

    // Adopt an accepted fd (the acceptor leaf's accept() result); set non-blocking so the channel's
    // poll() drains recv() cooperatively, mirroring the dialed socket's set_nonblocking().
    explicit lwip_socket(int fd)
            : m_fd(fd)
            , m_closed(false)
            , m_blocking(false)
            , m_connect_outcome(std::nullopt)
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
            , m_closed(other.m_closed.load())
            , m_blocking(other.m_blocking)
            , m_connect_outcome(other.m_connect_outcome)
    {
        other.m_fd = -1;
        other.m_closed.store(true);
    }

    lwip_socket &operator=(lwip_socket &&other) noexcept
    {
        if(this != &other)
        {
            close();
            m_fd = other.m_fd;
            m_closed.store(other.m_closed.load());
            m_blocking        = other.m_blocking;
            m_connect_outcome = other.m_connect_outcome;
            other.m_fd        = -1;
            other.m_closed.store(true);
        }
        return *this;
    }

    std::error_code connect(endpoint_type ep)
    {
        const std::optional<sockaddr_in> addr = ipv4_address(parse_host_port(ep.address));
        if(!addr)
            return std::make_error_code(std::errc::invalid_argument);
        m_fd = ::lwip_socket(AF_INET, SOCK_STREAM, 0); // the BSD opener; the class name shadows it unqualified here
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        set_nonblocking();
        if(lwip_connect(m_fd, reinterpret_cast<const sockaddr *>(&*addr), sizeof(*addr)) < 0 && errno != EINPROGRESS)
            return std::error_code{errno, std::generic_category()};
        m_closed.store(false);
        return {};
    }

    std::optional<std::error_code> connect_outcome()
    {
        if(!m_connect_outcome)
            m_connect_outcome = poll_connect(m_fd);
        return m_connect_outcome;
    }

    std::size_t send(std::span<const std::byte> bytes)
    {
        const int n = lwip_send(m_fd, bytes.data(), bytes.size(), MSG_DONTWAIT);
        return classify_io(n);
    }

    std::size_t recv(std::span<std::byte> buf)
    {
        const int n = lwip_recv(m_fd, buf.data(), buf.size(), m_blocking ? 0 : MSG_DONTWAIT);
        if(n == 0) // an orderly peer FIN is a hard drop
            m_closed.store(true);
        return classify_io(n);
    }

    bool closed() const
    {
        return m_closed.load();
    }

    void close()
    {
        if(m_fd >= 0)
            lwip_close(m_fd);
        m_fd = -1;
        m_closed.store(true);
    }

    // The RX-task receive policy parks in a blocking recv instead of spinning a non-blocking poll;
    // toggling the socket blocking is the P2 opt-in (P1 stays non-blocking for its cooperative drain).
    void set_blocking(bool blocking)
    {
        set_fd_blocking(m_fd, blocking);
        m_blocking = blocking;
    }

    void set_receive_timeout(std::chrono::milliseconds timeout)
    {
        apply_receive_timeout(m_fd, timeout);
    }

private:
    void set_nonblocking()
    {
        set_fd_blocking(m_fd, false);
    }

    // A negative return splits into two classes kept textually distinct. SOFT/transient
    // — EWOULDBLOCK/EAGAIN (send buffer full) or ENOMEM (lwIP ERR_MEM: a tcp_write/pbuf OOM) — is local
    // congestion: report 0, the channel re-arms, it NEVER tears down. HARD — ECONNRESET/EPIPE — is a
    // connection drop: set closed so the channel fires on_error and the engine re-dials.
    std::size_t classify_io(int n)
    {
        if(n > 0)
            return static_cast<std::size_t>(n);
        const bool soft = errno == EWOULDBLOCK || errno == EAGAIN || errno == ENOMEM;
        if(n < 0 && !soft && (errno == ECONNRESET || errno == EPIPE))
            m_closed.store(true);
        return 0;
    }

    int m_fd;
    std::atomic<bool> m_closed;
    bool m_blocking;
    std::optional<std::error_code> m_connect_outcome;
};

}

#endif

#endif

#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H

// A GENUINE POSIX-TCP stream_socket the host slice connects over loopback — NOT a mock and NOT a
// synthetic byte buffer. It exercises the channel's framing against real partial reads and real
// EWOULDBLOCK on a non-blocking 127.0.0.1 socket, so the loopback round-trip proves the dial path,
// the stream_inbound reassembly, and the P1 poll-drive end-to-end with no hardware. Test-local —
// never shipped in lib (on-target the lwIP socket in detail/lwip_socket_io.h plays this role).
#if !defined(ESP_PLATFORM)

    #include "host_tcp_socket_platform.h"

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
        suppress_sigpipe(m_fd);
        set_blocking(false);
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
            , m_stall(other.m_stall)
            , m_fault(other.m_fault)
    {
        other.m_fd     = -1;
        other.m_closed = true;
    }

    host_tcp_socket &operator=(host_tcp_socket &&other) noexcept
    {
        if(this != &other)
        {
            close();
            m_fd       = other.m_fd;
            m_closed   = other.m_closed;
            m_stall    = other.m_stall;
            m_fault    = other.m_fault;
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
        suppress_sigpipe(m_fd);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
        if(::connect(m_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0)
            return std::make_error_code(std::errc::connection_refused);
        set_blocking(false);
        m_closed = false;
        return {};
    }

    std::size_t send(std::span<const std::byte> bytes)
    {
        if(m_fault && *m_fault) // a test-forced HARD send drop: classify_io's ECONNRESET/EPIPE branch
        {
            m_closed = true;
            return 0;
        }
        if(m_stall && *m_stall) // a test-forced SOFT stall: the socket folded EWOULDBLOCK/ERR_MEM to 0
            return 0;
        const auto n = ::send(m_fd, bytes.data(), bytes.size(), nosignal_send_flag);
        return classify_io(static_cast<int>(n));
    }

    // Test scaffolding (host-only): borrow a test-owned flag to force a send class — SOFT stall (send
    // reports 0, channel re-arms) or HARD drop (send sets closed, channel fires on_error) — even after
    // the socket is moved into the channel (the pointer rides the move). The real loopback peer cannot
    // guarantee these deterministically (a closed peer's RST is not reliably timely on the host).
    void use_stall_flag(const bool &stall)
    {
        m_stall = &stall;
    }
    void use_fault_flag(const bool &fault)
    {
        m_fault = &fault;
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

    // The P2 RX-task receive policy parks in a blocking recv; P1 stays non-blocking for its drain.
    void set_blocking(bool blocking)
    {
        const int flags = ::fcntl(m_fd, F_GETFL, 0);
        ::fcntl(m_fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
    }

private:
    // Mirror the on-target lwip_socket::classify_io split: SOFT (EWOULDBLOCK/EAGAIN/ENOMEM) folds to 0
    // and re-arms; HARD (ECONNRESET/EPIPE) sets closed so the channel fires on_error.
    std::size_t classify_io(int n)
    {
        if(n > 0)
            return static_cast<std::size_t>(n);
        const bool soft = errno == EWOULDBLOCK || errno == EAGAIN || errno == ENOMEM;
        if(n < 0 && !soft && (errno == ECONNRESET || errno == EPIPE))
            m_closed = true;
        return 0;
    }

    int         m_fd;
    bool        m_closed;
    const bool *m_stall{nullptr};
    const bool *m_fault{nullptr};
};

}

#endif

#endif

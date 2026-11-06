#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_TCP_SOCKET_H

// A GENUINE POSIX-TCP stream_socket the host-side seam tests connect over loopback — NOT a mock and NOT a
// synthetic byte buffer. It exercises the channel's framing against real partial reads and real
// EWOULDBLOCK on a non-blocking 127.0.0.1 socket, so the loopback round-trip proves the dial path,
// the stream_inbound reassembly, and the P1 poll-drive end-to-end with no hardware. Test-local —
// never shipped in lib (on-target the lwIP socket in detail/lwip_socket_io.h plays this role).
#if !defined(ESP_PLATFORM)

    #include "host_socket_ops.h"
    #include "host_tcp_socket_platform.h"

    #include "plexus/io/endpoint.h"

    #include <sys/socket.h>

    #include <span>
    #include <atomic>
    #include <cerrno>
    #include <chrono>
    #include <thread>
    #include <cstddef>
    #include <system_error>

namespace plexus::test {

// A non-blocking connected TCP socket over loopback. A recv() of 0 with no transient errno means an
// orderly peer FIN (a hard drop the channel turns into on_error); EWOULDBLOCK is "nothing yet".
class host_tcp_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    host_tcp_socket()
            : m_fd(-1)
            , m_closed(false)
            , m_stall(nullptr)
            , m_fault(nullptr)
            , m_close_witness(nullptr)
    {
    }

    explicit host_tcp_socket(int fd)
            : m_fd(fd)
            , m_closed(false)
            , m_stall(nullptr)
            , m_fault(nullptr)
            , m_close_witness(nullptr)
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
            , m_closed(other.m_closed.load())
            , m_stall(other.m_stall)
            , m_fault(other.m_fault)
            , m_close_witness(other.m_close_witness)
    {
        other.m_fd = -1;
        other.m_closed.store(true);
    }

    host_tcp_socket &operator=(host_tcp_socket &&other) noexcept
    {
        if(this != &other)
        {
            close();
            m_fd = other.m_fd;
            m_closed.store(other.m_closed.load());
            m_stall         = other.m_stall;
            m_fault         = other.m_fault;
            m_close_witness = other.m_close_witness;
            other.m_fd      = -1;
            other.m_closed.store(true);
        }
        return *this;
    }

    std::error_code connect(endpoint_type ep)
    {
        m_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if(m_fd < 0)
            return std::make_error_code(std::errc::too_many_files_open);
        suppress_sigpipe(m_fd);
        if(connect_ipv4(m_fd, ep.address) < 0)
            return std::make_error_code(std::errc::connection_refused);
        set_blocking(false);
        m_closed.store(false);
        return {};
    }

    std::size_t send(std::span<const std::byte> bytes)
    {
        if(m_fault && *m_fault) // a test-forced HARD send drop: classify_io's ECONNRESET/EPIPE branch
        {
            m_closed.store(true);
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
    void use_close_witness(std::atomic<std::thread::id> &witness)
    {
        m_close_witness = &witness;
    }

    std::size_t recv(std::span<std::byte> buf)
    {
        const auto n = ::recv(m_fd, buf.data(), buf.size(), 0);
        if(n == 0)
            m_closed.store(true);
        return classify_io(static_cast<int>(n));
    }

    bool closed() const
    {
        return m_closed.load();
    }

    void close()
    {
        if(m_fd >= 0)
            close_fd(m_fd, m_close_witness);
        m_fd = -1;
        m_closed.store(true);
    }

    // The P2 RX-task receive policy parks in a blocking recv; P1 stays non-blocking for its drain.
    void set_blocking(bool blocking)
    {
        set_fd_blocking(m_fd, blocking);
    }

    void set_receive_timeout(std::chrono::milliseconds timeout)
    {
        apply_receive_timeout(m_fd, timeout);
    }

private:
    std::size_t classify_io(int n)
    {
        if(n > 0)
            return static_cast<std::size_t>(n);
        if(n < 0 && !soft_errno(errno) && hard_errno(errno))
            m_closed.store(true);
        return 0;
    }

    int m_fd;
    std::atomic<bool> m_closed;
    const bool *m_stall;
    const bool *m_fault;
    std::atomic<std::thread::id> *m_close_witness;
};

}

#endif

#endif

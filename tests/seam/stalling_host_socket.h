#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_STALLING_HOST_SOCKET_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_STALLING_HOST_SOCKET_H

#include "host_tcp_socket.h"

#include "plexus/io/endpoint.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <utility>
#include <optional>
#include <system_error>

namespace plexus::test {

// The transport default-constructs the sockets it dials, so the stall flag is bound at construction rather than
// injected afterwards; it lets the loopback egress be held resident deterministically, which the peer's timing cannot.
inline bool g_stall = false;

// The stall pointer rides host_tcp_socket's move, so it survives the transport's adopt().
class stalling_host_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    stalling_host_socket()
            : m_inner()
    {
        m_inner.use_stall_flag(g_stall);
    }

    stalling_host_socket(stalling_host_socket &&)            = default;
    stalling_host_socket &operator=(stalling_host_socket &&) = default;

    std::error_code connect(endpoint_type ep)
    {
        return m_inner.connect(std::move(ep));
    }
    std::optional<std::error_code> connect_outcome()
    {
        return m_inner.connect_outcome();
    }
    std::size_t send(std::span<const std::byte> bytes)
    {
        return m_inner.send(bytes);
    }
    std::size_t recv(std::span<std::byte> buf)
    {
        return m_inner.recv(buf);
    }
    bool closed() const
    {
        return m_inner.closed();
    }
    void close()
    {
        m_inner.close();
    }
    void set_blocking(bool blocking)
    {
        m_inner.set_blocking(blocking);
    }
    bool set_receive_timeout(std::chrono::milliseconds timeout)
    {
        return m_inner.set_receive_timeout(timeout);
    }

private:
    host_tcp_socket m_inner;
};

}

#endif

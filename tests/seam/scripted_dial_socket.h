#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_SCRIPTED_DIAL_SOCKET_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_SCRIPTED_DIAL_SOCKET_H

#include "plexus/io/endpoint.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <system_error>

namespace plexus::test {

struct dial_script
{
    std::optional<std::error_code> outcome = std::nullopt;
    std::error_code start                  = {};
    bool accept_ready                      = false;
    bool receive_timeout_accepted          = true;
    std::uint32_t connects                 = 0;
    std::uint32_t closes                   = 0;
    std::uint32_t probes                   = 0;
};

// The transport default-constructs the sockets it dials, so the script is global rather than injected per socket.
inline dial_script g_dial_script;

class scripted_dial_socket
{
public:
    using endpoint_type = plexus::io::endpoint;

    scripted_dial_socket()
            : scripted_dial_socket(false)
    {
    }

    explicit scripted_dial_socket(bool open)
            : m_open(open)
    {
    }

    ~scripted_dial_socket()
    {
        close();
    }

    scripted_dial_socket(const scripted_dial_socket &)            = delete;
    scripted_dial_socket &operator=(const scripted_dial_socket &) = delete;

    scripted_dial_socket(scripted_dial_socket &&other) noexcept
            : m_open(std::exchange(other.m_open, false))
    {
    }

    scripted_dial_socket &operator=(scripted_dial_socket &&other) noexcept
    {
        if(this != &other)
        {
            close();
            m_open = std::exchange(other.m_open, false);
        }
        return *this;
    }

    std::error_code connect(endpoint_type)
    {
        ++g_dial_script.connects;
        m_open = !g_dial_script.start;
        return g_dial_script.start;
    }

    std::optional<std::error_code> connect_outcome()
    {
        ++g_dial_script.probes;
        return g_dial_script.outcome;
    }

    std::size_t send(std::span<const std::byte>)
    {
        return 0;
    }

    std::size_t recv(std::span<std::byte>)
    {
        return 0;
    }

    bool closed() const
    {
        return !m_open;
    }

    void close()
    {
        if(m_open)
            ++g_dial_script.closes;
        m_open = false;
    }

    void set_blocking(bool)
    {
    }

    bool set_receive_timeout(std::chrono::milliseconds)
    {
        return g_dial_script.receive_timeout_accepted;
    }

private:
    bool m_open;
};

// The transport default-constructs its acceptor as well, so whether a connection waits lives in the global script.
class scripted_acceptor
{
public:
    std::error_code bind_and_listen(plexus::io::endpoint)
    {
        return {};
    }

    std::optional<scripted_dial_socket> accept_one()
    {
        if(!std::exchange(g_dial_script.accept_ready, false))
            return std::nullopt;
        return scripted_dial_socket{true};
    }

    void close()
    {
    }
};

}

#endif

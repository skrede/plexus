#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ACCEPT_RETRY_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ACCEPT_RETRY_H

#include "plexus/io/detail/pending_ops.h"

#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <system_error>

namespace plexus::asio::detail {

enum class accept_verdict : std::uint8_t
{
    stop,
    rearm,
    backoff
};

inline bool is_dead_acceptor(const std::error_code &ec)
{
    return ec == ::asio::error::operation_aborted || ec == ::asio::error::bad_descriptor || ec == ::asio::error::invalid_argument || ec == ::asio::error::not_socket ||
            ec == std::errc::operation_canceled || ec == std::errc::bad_file_descriptor || ec == std::errc::invalid_argument || ec == std::errc::not_a_socket;
}

// accept(2) names these as network errors already pending on the new connection, which one call consumes. Windows
// AcceptEx keeps failing with WSAENETDOWN while an interface is down, so there network_down is not one of them.
inline bool is_pending_connection_error(const std::error_code &ec)
{
#if !defined(_WIN32)
    if(ec == ::asio::error::network_down || ec == std::errc::network_down)
        return true;
#endif
#if defined(__linux__)
    // Linux accept(2) NOTES list these too; BSD and Windows accept do not pass pending network errors through.
    if(ec == std::errc::no_protocol_option || ec == std::error_code(EHOSTDOWN, std::system_category()) || ec == std::error_code(ENONET, std::system_category()))
        return true;
#endif
    return ec == ::asio::error::connection_aborted || ec == std::errc::connection_aborted || ec == ::asio::error::connection_reset || ec == std::errc::connection_reset ||
            ec == ::asio::error::network_unreachable || ec == std::errc::network_unreachable || ec == ::asio::error::host_unreachable || ec == std::errc::host_unreachable ||
            ec == std::errc::protocol_error || ec == std::errc::operation_not_permitted;
}

// Any error but a pending connection's may persist on the listening socket, so it re-arms through the backoff rather
// than spinning the io_context. Each code is matched as asio raises it and as its portable errc.
inline accept_verdict classify_accept_error(const std::error_code &ec)
{
    if(is_dead_acceptor(ec))
        return accept_verdict::stop;
    if(is_pending_connection_error(ec))
        return accept_verdict::rearm;
    return accept_verdict::backoff;
}

// The delay doubles when a wait is armed, so delay() is the one the next wait will use.
class accept_backoff
{
public:
    explicit accept_backoff(::asio::io_context &io)
            : m_timer(io)
            , m_ops()
            , m_delay(initial_delay)
    {
    }

    accept_backoff(const accept_backoff &)            = delete;
    accept_backoff &operator=(const accept_backoff &) = delete;

    template<typename Fn>
    void wait(Fn fn)
    {
        m_timer.expires_after(m_delay);
        m_delay = std::min(m_delay * 2, max_delay);
        m_ops.begin();
        m_timer.async_wait(
                [this, fn = std::move(fn)](const std::error_code &ec) mutable
                {
                    m_ops.end();
                    if(!ec)
                        fn();
                });
    }

    void reset() noexcept
    {
        m_delay = initial_delay;
    }

    void cancel()
    {
        m_timer.cancel();
    }

    std::size_t pending() const noexcept
    {
        return m_ops.count();
    }

    std::chrono::milliseconds delay() const noexcept
    {
        return m_delay;
    }

private:
    static constexpr std::chrono::milliseconds initial_delay{5};
    static constexpr std::chrono::milliseconds max_delay{1000};

    ::asio::steady_timer m_timer;
    io::detail::pending_ops m_ops;
    std::chrono::milliseconds m_delay;
};

// A stopped listener still reports a late error but arms nothing, so no wait outlives the stop() that cancelled the
// last one.
template<typename Report, typename Rearm>
void handle_accept_error(accept_backoff &backoff, const std::error_code &ec, bool running, Report &&report, Rearm &&rearm)
{
    if(ec != ::asio::error::operation_aborted)
        report(ec);
    if(!running)
        return;
    switch(classify_accept_error(ec))
    {
        case accept_verdict::stop:
            return;
        case accept_verdict::rearm:
            return rearm();
        case accept_verdict::backoff:
            return backoff.wait(std::forward<Rearm>(rearm));
    }
}

}

#endif

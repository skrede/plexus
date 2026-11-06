#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ACCEPT_RETRY_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ACCEPT_RETRY_H

#include "plexus/io/detail/pending_ops.h"

#include <asio/error.hpp>
#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

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

// accept(2) asks a server to treat the network errors already pending on a new connection like EAGAIN and retry, so
// only a dead or cancelled acceptor stops the loop. Each code is matched as asio raises it and as its portable errc.
inline accept_verdict classify_accept_error(const std::error_code &ec)
{
    if(ec == ::asio::error::operation_aborted || ec == ::asio::error::bad_descriptor || ec == ::asio::error::invalid_argument || ec == ::asio::error::not_socket ||
       ec == std::errc::operation_canceled || ec == std::errc::bad_file_descriptor || ec == std::errc::invalid_argument || ec == std::errc::not_a_socket)
        return accept_verdict::stop;
    if(ec == ::asio::error::no_descriptors || ec == ::asio::error::no_buffer_space || ec == ::asio::error::no_memory || ec == std::errc::too_many_files_open ||
       ec == std::errc::too_many_files_open_in_system || ec == std::errc::no_buffer_space || ec == std::errc::not_enough_memory)
        return accept_verdict::backoff;
    return accept_verdict::rearm;
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

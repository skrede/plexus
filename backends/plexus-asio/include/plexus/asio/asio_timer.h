#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_TIMER_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_TIMER_H

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <utility>
#include <system_error>

namespace plexus::asio {

// steady_timer wrapper satisfying the plexus::timer concept. Mirrors mdnspp's
// AsioTimer: both ctors take the executor (the io_context the Policy carries),
// the second the constructibility constraint the timer concept requires; the
// surface forwards straight onto ::asio::steady_timer.
class asio_timer
{
public:
    explicit asio_timer(::asio::io_context &io)
            : m_timer(io)
    {
    }

    asio_timer(::asio::io_context &io, std::error_code &)
            : m_timer(io)
    {
    }

    void expires_after(std::chrono::milliseconds dur) { m_timer.expires_after(dur); }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)> handler)
    {
        m_timer.async_wait(std::move(handler));
    }

    void cancel() { m_timer.cancel(); }

private:
    ::asio::steady_timer m_timer;
};

}

static_assert(plexus::timer<plexus::asio::asio_timer>,
              "asio_timer must satisfy plexus::timer — check expires_after/async_wait/cancel");

#endif

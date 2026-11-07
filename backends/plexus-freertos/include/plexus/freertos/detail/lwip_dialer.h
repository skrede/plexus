#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_DIALER_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_DIALER_H

#include "plexus/freertos/detail/lwip_errors.h"

#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/stream/stream_socket.h"

#include "plexus/detail/compat.h"

#include <array>
#include <chrono>
#include <limits>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <system_error>

namespace plexus::freertos::detail {

template<plexus::stream::stream_socket Socket, std::size_t N>
class lwip_dialer
{
    struct pending
    {
        Socket socket;
        plexus::io::endpoint ep;
        TickType_t started;
        TickType_t timeout;
    };

public:
    using failed_cb = plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>;

    explicit lwip_dialer(freertos_executor &ex)
            : m_executor(ex)
            , m_pending()
            , m_count(0)
            , m_timeout(0)
            , m_on_failed_cb()
    {
    }

    void on_failed(failed_cb cb)
    {
        m_on_failed_cb = std::move(cb);
    }

    void set_timeout(std::chrono::nanoseconds timeout) noexcept
    {
        m_timeout = to_ticks(timeout);
    }

    void report(const plexus::io::endpoint &ep, plexus::io::io_error e)
    {
        if(m_on_failed_cb)
            m_on_failed_cb(ep, e);
    }

    // The owner closes the transport and drains the executor before destroying it, as for the asio transports.
    void refuse(const plexus::io::endpoint &ep, plexus::io::io_error e)
    {
        m_executor.post([this, ep, e] { report(ep, e); });
    }

    void dial(const plexus::io::endpoint &ep)
    {
        if(m_count == N)
            return refuse(ep, plexus::io::io_error::would_block);
        Socket socket;
        if(std::error_code ec = socket.connect(ep); ec)
            return refuse(ep, map_error(ec));
        m_pending[m_count++] = pending{std::move(socket), ep, xTaskGetTickCount(), m_timeout};
    }

    // An entry leaves the table before its callback runs, so a callback may dial again. A connect that resolved is
    // reported as resolved even on the pass its timeout elapsed.
    template<typename Connected>
    void complete_each(Connected &&on_connected)
    {
        for(std::size_t i = 0; i < m_count;)
        {
            const std::optional<std::error_code> outcome = m_pending[i].socket.connect_outcome();
            if(!outcome && !expired(m_pending[i]))
            {
                ++i;
                continue;
            }
            pending done = take(i);
            if(!outcome)
            {
                done.socket.close();
                report(done.ep, plexus::io::io_error::timed_out);
            }
            else if(*outcome)
                report(done.ep, map_error(*outcome));
            else
                on_connected(std::move(done.socket), done.ep);
        }
    }

    void abort_all()
    {
        for(std::size_t i = 0; i < m_count; ++i)
        {
            m_pending[i].socket.close();
            refuse(m_pending[i].ep, plexus::io::io_error::operation_aborted);
        }
        m_count = 0;
    }

private:
    // FreeRTOS's pdMS_TO_TICKS multiplies in TickType_t, so the count is capped where a 1000 Hz tick still fits 32 bits.
    static TickType_t to_ticks(std::chrono::nanoseconds timeout) noexcept
    {
        if(timeout <= std::chrono::nanoseconds::zero())
            return 0;
        constexpr std::int64_t max_ms = std::numeric_limits<std::uint32_t>::max() / 1000;
        const std::int64_t ms         = std::min<std::int64_t>(std::chrono::ceil<std::chrono::milliseconds>(timeout).count(), max_ms);
        return std::max<TickType_t>(pdMS_TO_TICKS(static_cast<std::uint32_t>(ms)), 1);
    }

    // The tick count can stand just short of its next step at the dial, so only a count past the timeout proves the
    // whole timeout elapsed. Unsigned subtraction keeps the comparison right across a tick wrap.
    static bool expired(const pending &p) noexcept
    {
        return p.timeout != 0 && static_cast<TickType_t>(xTaskGetTickCount() - p.started) > p.timeout;
    }

    pending take(std::size_t i)
    {
        pending done = std::move(m_pending[i]);
        if(i != --m_count)
            m_pending[i] = std::move(m_pending[m_count]);
        return done;
    }

    freertos_executor &m_executor;
    std::array<pending, N> m_pending;
    std::size_t m_count;
    TickType_t m_timeout;
    failed_cb m_on_failed_cb;
};

}

#endif

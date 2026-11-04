#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_DIAL_DEADLINE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_DIAL_DEADLINE_H

#include "plexus/detail/compat.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <chrono>
#include <utility>
#include <system_error>
#include <unordered_map>

namespace plexus::asio::detail {

template<typename Channel>
class dial_deadline
{
public:
    explicit dial_deadline(::asio::io_context &io)
            : m_io(io)
            , m_timers()
    {
    }

    dial_deadline(const dial_deadline &)            = delete;
    dial_deadline &operator=(const dial_deadline &) = delete;

    // A handler returns on an aborted wait before touching the table, which is what lets close and destruction cancel
    // safely.
    void arm(Channel *raw, std::chrono::nanoseconds timeout, plexus::detail::move_only_function<void()> on_expire)
    {
        if(timeout <= std::chrono::nanoseconds::zero())
            return;
        auto &timer = m_timers.try_emplace(raw, m_io).first->second;
        timer.expires_after(timeout);
        timer.async_wait(
                [this, raw, on_expire = std::move(on_expire)](std::error_code ec) mutable
                {
                    if(!ec)
                        expire(raw, on_expire);
                });
    }

    void disarm(Channel *raw)
    {
        m_timers.erase(raw);
    }

    void cancel_all()
    {
        m_timers.clear();
    }

private:
    ::asio::io_context &m_io;
    std::unordered_map<Channel *, ::asio::steady_timer> m_timers;

    // An expiry already queued when its dial resolved still completes without error, and the address may by then key a
    // later dial, so only an entry whose own timer has run out expires.
    void expire(Channel *raw, plexus::detail::move_only_function<void()> &on_expire)
    {
        auto it = m_timers.find(raw);
        if(it == m_timers.end() || it->second.expiry() > ::asio::steady_timer::clock_type::now())
            return;
        m_timers.erase(it);
        on_expire();
    }
};

}

#endif

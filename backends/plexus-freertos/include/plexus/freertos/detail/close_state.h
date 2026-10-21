#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_CLOSE_STATE_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_CLOSE_STATE_H

#include "plexus/freertos/freertos_executor.h"

#include "plexus/io/detail/close_notification.h"

#include "plexus/detail/compat.h"

#include <utility>

namespace plexus::freertos::detail {

// A constrained-target channel's closed state and its on_closed, posted through the executor the channel runs on. The
// shutdown hook runs synchronously when the channel closes, for the transport that polls it, so on_closed stays the
// owner's alone.
class close_state
{
public:
    explicit close_state(freertos_executor &ex) noexcept
            : m_ex(ex)
            , m_closed(false)
            , m_notification()
            , m_on_shutdown_cb()
    {
    }

    close_state(const close_state &)            = delete;
    close_state &operator=(const close_state &) = delete;

    bool closed() const noexcept
    {
        return m_closed;
    }

    // True only for the call that moves the channel into the closed state.
    bool enter() noexcept
    {
        return !std::exchange(m_closed, true);
    }

    void on_shutdown(plexus::detail::move_only_function<void()> cb)
    {
        m_on_shutdown_cb = std::move(cb);
    }

    void shut_down()
    {
        if(auto cb = std::exchange(m_on_shutdown_cb, nullptr))
            cb();
    }

    void install(plexus::detail::move_only_function<void()> cb)
    {
        m_notification.install(std::move(cb));
        if(m_closed)
            notify();
    }

    void notify()
    {
        m_notification.notify([this](plexus::detail::move_only_function<void()> fn) { m_ex.post(std::move(fn)); });
    }

private:
    freertos_executor &m_ex;
    bool m_closed;
    plexus::io::detail::close_notification m_notification;
    plexus::detail::move_only_function<void()> m_on_shutdown_cb;
};

}

#endif

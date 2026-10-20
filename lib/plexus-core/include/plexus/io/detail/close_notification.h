#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_CLOSE_NOTIFICATION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_CLOSE_NOTIFICATION_H

#include "plexus/detail/compat.h"

#include <utility>

namespace plexus::io::detail {

// A channel's on_closed handler and its posted-once latch. The channel calls notify() whenever it may have gone quiet
// and again from its on_closed() setter, so a late install is posted too. The posted closure carries the handler,
// never the channel, so it cannot reach a channel its owner already destroyed.
class close_notification
{
public:
    close_notification() noexcept
            : m_posted(false)
            , m_on_closed_cb()
    {
    }

    close_notification(const close_notification &)            = delete;
    close_notification &operator=(const close_notification &) = delete;

    void install(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed_cb = std::move(cb);
    }

    template<typename Post>
    void notify(Post &&post)
    {
        if(m_posted || !m_on_closed_cb)
            return;
        m_posted = true;
        std::forward<Post>(post)([cb = std::exchange(m_on_closed_cb, nullptr)]() mutable { cb(); });
    }

private:
    bool m_posted;
    plexus::detail::move_only_function<void()> m_on_closed_cb;
};

}

#endif

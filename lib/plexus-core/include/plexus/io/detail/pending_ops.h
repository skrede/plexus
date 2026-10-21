#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PENDING_OPS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PENDING_OPS_H

#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"

#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// Counts an owner's issued-but-uncompleted operations: every I/O initiation, post and timer wait. The settle hook
// runs synchronously, at most once, so a hook that can lead to destroying anything must post rather than destroy.
class pending_ops
{
public:
    pending_ops() noexcept
            : m_closing(false)
            , m_settled(false)
            , m_count(0)
            , m_settled_cb()
    {
    }

    pending_ops(const pending_ops &)            = delete;
    pending_ops &operator=(const pending_ops &) = delete;

    void begin() noexcept
    {
        ++m_count;
    }

    void end()
    {
        if(m_count == 0)
            plexus::detail::fail_closed("pending_ops: end() with nothing outstanding");
        --m_count;
        settle();
    }

    void close()
    {
        m_closing = true;
        settle();
    }

    void on_settled(plexus::detail::move_only_function<void()> cb)
    {
        m_settled_cb = std::move(cb);
        if(m_settled)
            run_settled_cb();
    }

    std::size_t count() const noexcept
    {
        return m_count;
    }

    bool closing() const noexcept
    {
        return m_closing;
    }

    bool settled() const noexcept
    {
        return m_settled;
    }

private:
    bool m_closing;
    bool m_settled;
    std::size_t m_count;
    plexus::detail::move_only_function<void()> m_settled_cb;

    void settle()
    {
        if(m_settled || !m_closing || m_count != 0)
            return;
        m_settled = true;
        run_settled_cb();
    }

    void run_settled_cb()
    {
        auto cb = std::exchange(m_settled_cb, nullptr);
        if(cb)
            cb();
    }
};

// The component counts as one operation of its owner until it settles. The hook is installed when the owner closes,
// after the begin, so an owner destroyed without closing never ends a count it did not begin.
inline void hold_until_settled(pending_ops &owner, pending_ops &component)
{
    owner.begin();
    component.on_settled([&owner] { owner.end(); });
}

}

#endif

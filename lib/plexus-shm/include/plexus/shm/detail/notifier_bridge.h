#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_NOTIFIER_BRIDGE_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_NOTIFIER_BRIDGE_H

#include "plexus/detail/compat.h"

#include <memory>
#include <utility>

namespace plexus::shm::detail {

// The user's executor and the armed drain every notifier carries. A notifier shares it with its
// in-flight handlers and posted drains, so a completion that runs after disarm finds a live bridge
// with no drain instead of a destroyed notifier.
template<typename Policy>
class notifier_bridge
{
public:
    using executor_type = typename Policy::executor_type;
    using drain_fn      = plexus::detail::move_only_function<void()>;

    explicit notifier_bridge(executor_type executor) noexcept
            : m_drain_cb()
            , m_executor(executor)
    {
    }

    executor_type executor() const noexcept
    {
        return m_executor;
    }

    void arm(drain_fn drain) noexcept
    {
        m_drain_cb = std::move(drain);
    }

    void disarm() noexcept
    {
        m_drain_cb = nullptr;
    }

    void drain()
    {
        if(m_drain_cb)
            m_drain_cb();
    }

private:
    drain_fn m_drain_cb;
    executor_type m_executor;
};

// Posts one drain onto the bridge's executor (Policy::post, never a serialized wrapper). The
// closure holds the owner of the bridge, never the notifier.
template<typename Policy, typename Owner>
void post_drain(std::shared_ptr<Owner> owner, notifier_bridge<Policy> &bridge)
{
    std::shared_ptr<notifier_bridge<Policy>> held(std::move(owner), &bridge);
    Policy::post(bridge.executor(), [held = std::move(held)] { held->drain(); });
}

}

#endif

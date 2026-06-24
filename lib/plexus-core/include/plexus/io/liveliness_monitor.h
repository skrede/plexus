#ifndef HPP_GUARD_PLEXUS_IO_LIVELINESS_MONITOR_H
#define HPP_GUARD_PLEXUS_IO_LIVELINESS_MONITOR_H

#include "plexus/policy.h"
#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include "plexus/io/liveness_event.h"

#include "plexus/io/detail/liveliness_scan.h"
#include "plexus/io/detail/endpoint_liveness.h"

#include <map>
#include <chrono>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

constexpr std::chrono::milliseconds k_tick_granularity{100};

template<typename Policy, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy>
class liveliness_monitor
{
public:
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;
    using deadline_key  = detail::deadline_key;

    explicit liveliness_monitor(executor_type executor)
            : m_executor(executor)
            , m_tick(m_executor)
            , m_granularity(k_tick_granularity)
    {
    }

    void start()
    {
        arm_tick();
    }

    // A 0 period leaves the axis inert (it never fires).
    void register_endpoint(const node_id &id, std::uint64_t topic_hash, std::uint64_t deadline_period_ns, std::uint64_t lease_ns)
    {
        const std::uint64_t now = now_ns();
        auto &d                 = m_deadlines[deadline_key{id, topic_hash}];
        d.deadline_period_ns    = deadline_period_ns;
        d.last_data_seen_ns     = now;
        d.deadline_violated     = false;
        auto &l                 = m_leases[id];
        l.lease_ns              = lease_ns;
        l.last_seen_ns          = now;
        l.lease_expired         = false;
    }

    void deregister_endpoint(const node_id &id)
    {
        for(auto it = m_deadlines.begin(); it != m_deadlines.end();)
            it = (it->first.first == id) ? m_deadlines.erase(it) : std::next(it);
        m_leases.erase(id);
    }

    // A data frame asserts both deadline-progress (per-topic) and presence (per-endpoint).
    void stamp_data(const node_id &id, std::uint64_t topic_hash)
    {
        const std::uint64_t now = now_ns();
        if(auto it = m_deadlines.find(deadline_key{id, topic_hash}); it != m_deadlines.end())
        {
            it->second.last_data_seen_ns = now;
            it->second.deadline_violated = false;
        }
        stamp_seen(id);
    }

    // A heartbeat asserts presence ONLY (per-endpoint): it must NOT refresh the deadline clock,
    // so a heartbeat never masks a genuine missed deadline.
    void stamp_seen(const node_id &id)
    {
        if(auto it = m_leases.find(id); it != m_leases.end())
        {
            it->second.last_seen_ns  = now_ns();
            it->second.lease_expired = false;
        }
    }

    void on_liveness(plexus::detail::move_only_function<void(const liveness_event &)> cb)
    {
        m_on_liveness = std::move(cb);
    }

    // Runs on the SAME single tick — NOT a second timer.
    void on_tick_action(plexus::detail::move_only_function<void()> cb)
    {
        m_on_tick_action = std::move(cb);
    }

private:
    std::uint64_t now_ns() const
    {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
    }

    void arm_tick()
    {
        m_tick.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(m_granularity));
        m_tick.async_wait(
                [this](std::error_code ec)
                {
                    if(!ec)
                        on_tick();
                });
    }

    void on_tick()
    {
        detail::scan_liveness(m_deadlines, m_leases, now_ns(), [this](const liveness_event &ev) { fire(ev); });
        if(m_on_tick_action)
            m_on_tick_action();
        arm_tick();
    }

    void fire(const liveness_event &ev)
    {
        if(m_on_liveness)
            m_on_liveness(ev);
    }

    executor_type m_executor;
    timer_type m_tick;
    std::chrono::nanoseconds m_granularity;
    std::map<deadline_key, detail::endpoint_liveness> m_deadlines;
    std::map<node_id, detail::endpoint_liveness> m_leases;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness;
    plexus::detail::move_only_function<void()> m_on_tick_action;
};

}

#endif

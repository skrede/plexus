#ifndef HPP_GUARD_PLEXUS_IO_LIVELINESS_MONITOR_H
#define HPP_GUARD_PLEXUS_IO_LIVELINESS_MONITOR_H

#include "plexus/io/liveness_event.h"
#include "plexus/io/detail/endpoint_liveness.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <map>
#include <chrono>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io {

// The benchmark-pending periodic-check cadence: the single ticker re-arms at this
// granularity and the on_tick scan runs once per tick. 100ms is the research-assumed
// starting point — the lapse latency is bounded by one granularity, and the cost is
// O(registered endpoints) per tick, flat in message rate. The value is to be
// substantiated at the fan-out benchmark, not fixed by feel. A requested period
// below the granularity can never cross a tick boundary, so callers pin every
// period >= this (the static_assert in the unit oracle enforces it).
constexpr std::chrono::milliseconds k_tick_granularity{100};

// The ONE router-level periodic-check ticker the timing gates are built on. It owns
// EXACTLY one timer (m_tick), constructed from the borrowed executor, re-armed once
// per tick TOTAL. The hot receive path never arms a timer: it calls stamp_data /
// stamp_seen, each a plain store into resident per-endpoint state. The scale is the
// endpoint count, not the message rate (the timer-storm anti-pattern is structurally
// impossible).
//
// LIFETIME: the monitor borrows the executor as the substrate the tick posts onto;
// it owns no io_context, no thread, no shared_from_this, no atomics. The engine
// quiesces the executor before teardown and deregisters an endpoint on the
// disconnect lifecycle edge, so a firing tick reads only resident state and never
// touches a dead endpoint.
//
// TWO-KEY DESIGN: the DEADLINE state is keyed per-(endpoint, topic_hash) — a data
// gap is per-topic — while the LEASE state is keyed per-endpoint — presence is
// session-level. register_endpoint grows the per-(endpoint, topic) deadline entry
// AND ensures the per-endpoint lease entry; deregister_endpoint erases every deadline
// entry for the endpoint AND its lease entry (peer death tears the whole endpoint
// down). A scan iterates only the live maps, so a fire never touches a removed key.
template<typename Policy, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy>
class liveliness_monitor
{
public:
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;
    using deadline_key  = std::pair<node_id, std::uint64_t>;

    explicit liveliness_monitor(executor_type executor)
            : m_executor(executor)
            , m_tick(m_executor)
            , m_granularity(k_tick_granularity)
    {
    }

    // Arm the single ticker. From here it re-arms itself once per tick.
    void start() { arm_tick(); }

    // Grow this endpoint's per-topic deadline state and ensure its per-endpoint lease
    // state. A 0 period leaves the axis inert (it never fires). Cold path.
    void register_endpoint(const node_id &id, std::uint64_t topic_hash,
                           std::uint64_t deadline_period_ns, std::uint64_t lease_ns)
    {
        const std::uint64_t now = now_ns();
        auto               &d   = m_deadlines[deadline_key{id, topic_hash}];
        d.deadline_period_ns    = deadline_period_ns;
        d.last_data_seen_ns     = now;
        d.deadline_violated     = false;
        auto &l                 = m_leases[id];
        l.lease_ns              = lease_ns;
        l.last_seen_ns          = now;
        l.lease_expired         = false;
    }

    // Erase ALL of this endpoint's deadline entries and its lease entry (peer death).
    void deregister_endpoint(const node_id &id)
    {
        for(auto it = m_deadlines.begin(); it != m_deadlines.end();)
            it = (it->first.first == id) ? m_deadlines.erase(it) : std::next(it);
        m_leases.erase(id);
    }

    // A data frame asserts both deadline-progress (per-topic) AND presence
    // (per-endpoint). A plain store that also re-arms the deadline latch so a
    // resumed flow re-fires on a later lapse; arms no timer.
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

    // A heartbeat asserts presence ONLY (per-endpoint) — it must NOT refresh the
    // deadline clock, so a heartbeat never masks a genuine missed deadline. A plain
    // store that also re-arms the lease latch so a resumed presence re-fires on a
    // later lapse; arms no timer.
    void stamp_seen(const node_id &id)
    {
        if(auto it = m_leases.find(id); it != m_leases.end())
        {
            it->second.last_seen_ns  = now_ns();
            it->second.lease_expired = false;
        }
    }

    // The observable callback (absent = dormant). The engine sets it to route the
    // event up its observer seam.
    void on_liveness(plexus::detail::move_only_function<void(const liveness_event &)> cb)
    {
        m_on_liveness = std::move(cb);
    }

    // An additional per-tick action the engine attaches (the keepalive heartbeat
    // emit). It runs on the SAME single tick — NOT a second timer.
    void on_tick_action(plexus::detail::move_only_function<void()> cb)
    {
        m_on_tick_action = std::move(cb);
    }

private:
    // Stamp and scan against the SAME clock the ticker fires on (the virtual clock
    // under test, the steady clock in production), so an advance() that crosses a
    // tick expiry is the same elapsed gap the scan reads.
    std::uint64_t now_ns() const
    {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                                  Clock::now().time_since_epoch())
                                                  .count());
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

    // The edge-latched scan + the single re-arm. Each axis fires exactly once per
    // lapse and re-arms when data/presence resumes. The engine's per-tick action
    // runs on this same tick before the one re-arm.
    void on_tick()
    {
        const std::uint64_t now = now_ns();
        for(auto &[key, state] : m_deadlines)
            if(auto ev = check_deadline(key, state, now))
                fire(*ev);
        for(auto &[id, state] : m_leases)
            if(auto ev = check_lease(id, state, now))
                fire(*ev);
        if(m_on_tick_action)
            m_on_tick_action();
        arm_tick();
    }

    // Edge-latched deadline check: fire once when the data gap first exceeds the
    // period; clear the latch when the gap falls back under it.
    std::optional<liveness_event> check_deadline(const deadline_key        &key,
                                                 detail::endpoint_liveness &s, std::uint64_t now)
    {
        if(s.deadline_period_ns == 0)
            return std::nullopt;
        const bool lapsed = now - s.last_data_seen_ns > s.deadline_period_ns;
        if(lapsed && !s.deadline_violated)
        {
            s.deadline_violated = true;
            return liveness_event{liveness_kind::missed_deadline, key.first, key.second,
                                  s.deadline_period_ns};
        }
        if(!lapsed)
            s.deadline_violated = false;
        return std::nullopt;
    }

    // Edge-latched lease check: fire once when the presence gap first exceeds the
    // lease; clear the latch when presence resumes.
    std::optional<liveness_event> check_lease(const node_id &id, detail::endpoint_liveness &s,
                                              std::uint64_t now)
    {
        if(s.lease_ns == 0)
            return std::nullopt;
        const bool lapsed = now - s.last_seen_ns > s.lease_ns;
        if(lapsed && !s.lease_expired)
        {
            s.lease_expired = true;
            return liveness_event{liveness_kind::lease_expired, id, 0, s.lease_ns};
        }
        if(!lapsed)
            s.lease_expired = false;
        return std::nullopt;
    }

    void fire(const liveness_event &ev)
    {
        if(m_on_liveness)
            m_on_liveness(ev);
    }

    executor_type                                                    m_executor;
    timer_type                                                       m_tick;
    std::chrono::nanoseconds                                         m_granularity;
    std::map<deadline_key, detail::endpoint_liveness>                m_deadlines;
    std::map<node_id, detail::endpoint_liveness>                     m_leases;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness;
    plexus::detail::move_only_function<void()>                       m_on_tick_action;
};

}

#endif

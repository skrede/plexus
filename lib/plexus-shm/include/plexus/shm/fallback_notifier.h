#ifndef HPP_GUARD_PLEXUS_SHM_FALLBACK_NOTIFIER_H
#define HPP_GUARD_PLEXUS_SHM_FALLBACK_NOTIFIER_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/notifier_concept.h"

#include "plexus/detail/compat.h"

#include <cstdint>
#include <utility>
#include <concepts>

namespace plexus::shm {

enum class fallback_tier : std::uint8_t
{
    none,
    primary,
    secondary
};

// Arms the primary and falls back to the secondary when the primary cannot arm or later demotes. Each leg receives a
// forwarding drain, so a failed leg never consumes the waiter's drain. The legs hold the forwarding callbacks only
// while armed and disarm clears them, so the callbacks reach this object only while it is alive. A runtime demotion
// whose secondary cannot arm either is itself a demotion, so composites nest. Every move to the secondary, at arm or
// later, is reported once with the primary's reason; the report must not tear this notifier down.
template<typename Primary, typename Secondary>
    requires demotable_notifier<Primary> && notifier<Secondary>
class fallback_notifier
{
public:
    using drain_fn   = plexus::detail::move_only_function<void()>;
    using lost_fn    = plexus::detail::move_only_function<void(arm_status)>;
    using demoted_fn = plexus::detail::move_only_function<void(arm_status)>;

    template<typename... Args>
        requires std::constructible_from<Primary, Args &...> && std::constructible_from<Secondary, Args &...>
    explicit fallback_notifier(Args &&...args)
            : m_primary(args...)
            , m_secondary(args...)
            , m_drain_cb()
            , m_lost_cb()
            , m_demoted_cb()
            , m_slot(0)
            , m_engaged(fallback_tier::none)
            , m_primary_status(arm_status::armed)
    {
    }

    ~fallback_notifier()
    {
        disarm();
    }

    fallback_notifier(const fallback_notifier &)            = delete;
    fallback_notifier &operator=(const fallback_notifier &) = delete;
    fallback_notifier(fallback_notifier &&)                 = delete;
    fallback_notifier &operator=(fallback_notifier &&)      = delete;

    void signal() noexcept
    {
        if(m_engaged == fallback_tier::primary)
            m_primary.signal();
        else
            m_secondary.signal();
    }

    arm_status arm(std::uint32_t slot, drain_fn drain)
    {
        return arm(slot, std::move(drain), nullptr);
    }

    // lost runs once, on the executor, when a runtime demotion finds that the secondary cannot arm either.
    arm_status arm(std::uint32_t slot, drain_fn drain, lost_fn lost)
    {
        return arm(slot, std::move(drain), std::move(lost), nullptr);
    }

    // demoted runs once per move to the secondary: here when the primary cannot arm, or on the executor at a runtime
    // demotion, never from signal().
    arm_status arm(std::uint32_t slot, drain_fn drain, lost_fn lost, demoted_fn demoted)
    {
        m_slot           = slot;
        m_drain_cb       = std::move(drain);
        m_lost_cb        = std::move(lost);
        m_demoted_cb     = std::move(demoted);
        m_primary_status = m_primary.arm(slot, forward_drain(), [this](arm_status status) { demote(status); });
        if(m_primary_status == arm_status::armed)
        {
            m_engaged = fallback_tier::primary;
            return arm_status::armed;
        }
        m_primary.disarm();
        const arm_status fallback = arm_secondary();
        if(fallback == arm_status::armed)
            report_demotion();
        return fallback;
    }

    void disarm() noexcept
    {
        if(m_engaged == fallback_tier::primary)
            m_primary.disarm();
        else if(m_engaged == fallback_tier::secondary)
            m_secondary.disarm();
        m_engaged    = fallback_tier::none;
        m_drain_cb   = nullptr;
        m_lost_cb    = nullptr;
        m_demoted_cb = nullptr;
    }

    fallback_tier engaged() const noexcept
    {
        return m_engaged;
    }

    arm_status primary_status() const noexcept
    {
        return m_primary_status;
    }

private:
    Primary m_primary;
    Secondary m_secondary;
    drain_fn m_drain_cb;
    lost_fn m_lost_cb;
    demoted_fn m_demoted_cb;
    std::uint32_t m_slot;
    fallback_tier m_engaged;
    arm_status m_primary_status;

    drain_fn forward_drain()
    {
        return [this]
        {
            if(m_drain_cb)
                m_drain_cb();
        };
    }

    // The secondary's first cycle drains, so a message committed before the switch is not lost.
    arm_status arm_secondary()
    {
        const arm_status status = m_secondary.arm(m_slot, forward_drain());
        m_engaged               = status == arm_status::armed ? fallback_tier::secondary : fallback_tier::none;
        if(status != arm_status::armed)
            m_secondary.disarm();
        return status;
    }

    void report_demotion()
    {
        if(m_demoted_cb)
            m_demoted_cb(m_primary_status);
    }

    // The loss may destroy this notifier, so its callback runs from a local and nothing touches this afterwards.
    void demote(arm_status status)
    {
        if(m_engaged != fallback_tier::primary)
            return;
        m_primary.disarm();
        m_primary_status          = status;
        const arm_status fallback = arm_secondary();
        if(fallback == arm_status::armed)
            return report_demotion();
        if(!m_lost_cb)
            return;
        lost_fn lost = std::move(m_lost_cb);
        m_lost_cb    = nullptr;
        lost(fallback);
    }
};

}

#endif

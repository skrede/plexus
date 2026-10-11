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

// A notifier that can learn after arming that it cannot serve its waiter: it runs the injected demotion on the
// executor, after the handler that discovered it has returned.
template<typename T>
concept demotable_notifier = requires(T &n, std::uint32_t slot, plexus::detail::move_only_function<void()> drain, plexus::detail::move_only_function<void()> demote) {
    { n.signal() } -> std::same_as<void>;
    { n.arm(slot, std::move(drain), std::move(demote)) } -> std::same_as<arm_status>;
    { n.disarm() } -> std::same_as<void>;
};

// Arms the primary and falls back to the secondary when the primary cannot arm or later demotes. Each leg receives a
// forwarding drain, so a failed leg never consumes the waiter's drain. The legs hold the forwarding callbacks only
// while armed and disarm clears them, so the callbacks reach this object only while it is alive.
template<typename Primary, typename Secondary>
    requires demotable_notifier<Primary> && notifier<Secondary>
class fallback_notifier
{
public:
    using drain_fn = plexus::detail::move_only_function<void()>;

    template<typename... Args>
        requires std::constructible_from<Primary, Args &...> && std::constructible_from<Secondary, Args &...>
    explicit fallback_notifier(Args &&...args)
            : m_primary(args...)
            , m_secondary(args...)
            , m_drain_cb()
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
        m_slot           = slot;
        m_drain_cb       = std::move(drain);
        m_primary_status = m_primary.arm(slot, forward_drain(), [this] { demote(); });
        if(m_primary_status == arm_status::armed)
        {
            m_engaged = fallback_tier::primary;
            return arm_status::armed;
        }
        m_primary.disarm();
        return arm_secondary();
    }

    void disarm() noexcept
    {
        if(m_engaged == fallback_tier::primary)
            m_primary.disarm();
        else if(m_engaged == fallback_tier::secondary)
            m_secondary.disarm();
        m_engaged  = fallback_tier::none;
        m_drain_cb = nullptr;
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

    void demote()
    {
        if(m_engaged != fallback_tier::primary)
            return;
        m_primary.disarm();
        m_primary_status = arm_status::unsupported;
        arm_secondary();
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_BUS_NOTIFIER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_BUS_NOTIFIER_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/notifier_concept.h"

#include "plexus/detail/compat.h"

#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <functional>

namespace plexus::testing {

class bus_notifier;

// Every armed waiter of the process, so one signal on any ring drains each of them, the way a wake reaches a parked
// reader of another process.
struct wake_bus
{
    std::vector<std::reference_wrapper<bus_notifier>> armed;

    void ring();
};

class bus_notifier
{
public:
    explicit bus_notifier(wake_bus &bus) noexcept
            : m_bus(bus)
            , m_drain_cb()
    {
    }

    ~bus_notifier()
    {
        disarm();
    }

    bus_notifier(const bus_notifier &)            = delete;
    bus_notifier &operator=(const bus_notifier &) = delete;

    void signal() noexcept
    {
        m_bus.ring();
    }

    shm::arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()> drain)
    {
        m_drain_cb = std::move(drain);
        m_bus.armed.emplace_back(*this);
        return shm::arm_status::armed;
    }

    void disarm() noexcept
    {
        std::erase_if(m_bus.armed, [this](const bus_notifier &n) { return &n == this; });
        m_drain_cb = nullptr;
    }

    void drain()
    {
        if(m_drain_cb)
            m_drain_cb();
    }

private:
    wake_bus &m_bus;
    plexus::detail::move_only_function<void()> m_drain_cb;
};

inline void wake_bus::ring()
{
    const std::vector<std::reference_wrapper<bus_notifier>> snapshot = armed;
    for(bus_notifier &n : snapshot)
        if(std::ranges::any_of(armed, [&n](const bus_notifier &a) { return &a == &n; }))
            n.drain();
}

static_assert(shm::notifier<bus_notifier>);

}

#endif

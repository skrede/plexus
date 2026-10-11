#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_NOTIFIER_LEGS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_NOTIFIER_LEGS_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::testing {

using leg_fn = plexus::detail::move_only_function<void()>;

struct denied_primary
{
    denied_primary() noexcept
    {
    }

    template<typename Executor>
    denied_primary(Executor &&, plexus::shm::control_header_t &, std::string_view) noexcept
    {
    }

    void signal() noexcept
    {
    }

    plexus::shm::arm_status arm(std::uint32_t, leg_fn, leg_fn)
    {
        return plexus::shm::arm_status::denied;
    }

    void disarm() noexcept
    {
    }
};

struct denied_floor
{
    denied_floor() noexcept
    {
    }

    template<typename Executor>
    denied_floor(Executor &&, plexus::shm::control_header_t &, std::string_view) noexcept
    {
    }

    void signal() noexcept
    {
    }

    plexus::shm::arm_status arm(std::uint32_t, leg_fn)
    {
        return plexus::shm::arm_status::denied;
    }

    void disarm() noexcept
    {
    }
};

// Arms, then posts its demotion the way a primary reports an unsupported completion; disarm drops a demotion
// still in flight.
template<typename Policy>
class demoting_primary
{
public:
    demoting_primary(typename Policy::executor_type executor, plexus::shm::control_header_t &, std::string_view)
            : m_demote_cb(std::make_shared<leg_fn>())
            , m_executor(executor)
    {
    }

    void signal() noexcept
    {
    }

    plexus::shm::arm_status arm(std::uint32_t, leg_fn, leg_fn demote)
    {
        *m_demote_cb = std::move(demote);
        Policy::post(m_executor,
                     [held = m_demote_cb]
                     {
                         leg_fn demotion = std::move(*held);
                         *held           = nullptr;
                         if(demotion)
                             demotion();
                     });
        return plexus::shm::arm_status::armed;
    }

    void disarm() noexcept
    {
        *m_demote_cb = nullptr;
    }

private:
    std::shared_ptr<leg_fn> m_demote_cb;
    typename Policy::executor_type m_executor;
};

}

#endif

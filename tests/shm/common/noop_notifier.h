#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_NOOP_NOTIFIER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_NOOP_NOTIFIER_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/notifier_concept.h"

#include "plexus/detail/compat.h"

#include <cstdint>

namespace plexus::testing {

struct noop_notifier
{
    void signal() noexcept
    {
    }

    ::plexus::shm::arm_status arm(std::uint32_t, plexus::detail::move_only_function<void()>)
    {
        return ::plexus::shm::arm_status::armed;
    }

    void disarm() noexcept
    {
    }
};

static_assert(::plexus::shm::notifier<noop_notifier>);

}

#endif

#ifndef HPP_GUARD_PLEXUS_DETAIL_AFFINITY_CHECK_H
#define HPP_GUARD_PLEXUS_DETAIL_AFFINITY_CHECK_H

#include "plexus/detail/fail_closed.h"

#include "plexus/io/endpoint_seam.h"

#include <functional>
#include <type_traits>

namespace plexus::detail {

#if defined(PLEXUS_AFFINITY_CHECKS)
template<typename Policy>
void check_affinity(typename Policy::executor_type ex, const char *member) noexcept
{
    if(Policy::running_in_this_thread(ex))
        return;
    affinity_trip(member);
}

inline bool live_or_trip(bool live, const char *member) noexcept
{
    if(!live)
        moved_from_trip(member);
    return live;
}
#else
template<typename Policy>
void check_affinity(typename Policy::executor_type, const char *) noexcept
{
}

inline bool live_or_trip(bool live, const char *) noexcept
{
    return live;
}
#endif

inline bool moved_from(const io::endpoint_seam &seam) noexcept
{
    return seam.ctx == nullptr;
}

inline bool live_or_trip(const io::endpoint_seam &seam, const char *member) noexcept
{
    return live_or_trip(!moved_from(seam), member);
}

// How a closure that can outlive the object it came from keeps the executor: a reference executor_type as a
// reference_wrapper bound to the long-lived executor, a by-value executor_type as its own copy. Either is copyable.
template<typename Executor>
using held_executor = std::conditional_t<std::is_reference_v<Executor>, std::reference_wrapper<std::remove_reference_t<Executor>>, Executor>;

}

#endif

#ifndef HPP_GUARD_PLEXUS_DETAIL_AFFINITY_CHECK_H
#define HPP_GUARD_PLEXUS_DETAIL_AFFINITY_CHECK_H

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <type_traits>

namespace plexus::detail {

// A call off the driving thread is a bug in the caller, not a condition to recover from, so the check aborts rather
// than going through fail_closed.
#if defined(PLEXUS_AFFINITY_CHECKS)
template<typename Policy>
void check_affinity(typename Policy::executor_type ex, const char *member) noexcept
{
    if(Policy::running_in_this_thread(ex))
        return;
    std::fprintf(stderr, "plexus: %s called off the node's executor thread\n", member);
    std::abort();
}
#else
template<typename Policy>
void check_affinity(typename Policy::executor_type, const char *) noexcept
{
}
#endif

// How a closure that can outlive the object it came from keeps the executor: a reference executor_type as a
// reference_wrapper bound to the long-lived executor, a by-value executor_type as its own copy. Either is copyable.
template<typename Executor>
using held_executor = std::conditional_t<std::is_reference_v<Executor>, std::reference_wrapper<std::remove_reference_t<Executor>>, Executor>;

}

#endif

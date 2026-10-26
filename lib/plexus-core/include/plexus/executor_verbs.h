#ifndef HPP_GUARD_PLEXUS_EXECUTOR_VERBS_H
#define HPP_GUARD_PLEXUS_EXECUTOR_VERBS_H

#include "plexus/detail/compat.h"

#include <utility>

namespace plexus {

// The executor verbs of a Policy whose executor implements each verb as a member of the same name.
template<typename Executor>
struct executor_verbs
{
    using executor_type = Executor &;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }

    static bool post_from_thread(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        return ex.post_from_thread(std::move(fn));
    }

    static bool running_in_this_thread(executor_type ex)
    {
        return ex.running_in_this_thread();
    }

    static bool inside_executor(executor_type ex)
    {
        return ex.inside_executor();
    }

    static void release_driver(executor_type ex)
    {
        ex.release_driver();
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_EXECUTOR_VERBS_H
#define HPP_GUARD_PLEXUS_EXECUTOR_VERBS_H

#include "plexus/detail/compat.h"

#include <utility>

namespace plexus {

// The executor verbs of a Policy whose executor queues work through its own member post.
template<typename Executor>
struct executor_verbs
{
    using executor_type = Executor &;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_TESTING_INLINE_VERBS_H
#define HPP_GUARD_PLEXUS_TESTING_INLINE_VERBS_H

#include "plexus/detail/compat.h"

namespace plexus::testing {

// The executor verbs of a synchronous single-thread test executor that runs posted work inline: its caller is the only
// thread, and no handler is ever running when a verb is asked.
template<typename ExecutorType>
struct inline_verbs
{
    using executor_type = ExecutorType;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }

    static void post_from_thread(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }

    static bool running_in_this_thread(executor_type) noexcept
    {
        return true;
    }

    static bool inside_executor(executor_type) noexcept
    {
        return false;
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_IO_CONTEXT_VERBS_H
#define HPP_GUARD_PLEXUS_ASIO_IO_CONTEXT_VERBS_H

#include "plexus/asio/detail/executor_driver.h"

#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <thread>
#include <utility>

namespace plexus::asio {

// The executor verbs of a Policy driven by an asio io_context.
struct io_context_verbs
{
    using executor_type = ::asio::io_context &;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }

    // asio::post is safe from any thread, so the cross-thread ingress is the same queue.
    static void post_from_thread(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }

    static bool running_in_this_thread(executor_type ex)
    {
        detail::executor_driver &driver = ::asio::use_service<detail::executor_driver>(ex);
        return driver.observe(std::this_thread::get_id(), inside_executor(ex));
    }

    static bool inside_executor(executor_type ex) noexcept
    {
        return ex.get_executor().running_in_this_thread();
    }
};

}

#endif

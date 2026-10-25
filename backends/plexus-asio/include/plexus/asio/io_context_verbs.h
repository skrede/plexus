#ifndef HPP_GUARD_PLEXUS_ASIO_IO_CONTEXT_VERBS_H
#define HPP_GUARD_PLEXUS_ASIO_IO_CONTEXT_VERBS_H

#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

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
};

}

#endif

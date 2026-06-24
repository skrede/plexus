#ifndef HPP_GUARD_PLEXUS_MUXIFY_H
#define HPP_GUARD_PLEXUS_MUXIFY_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/polymorphic_byte_channel.h"

#include <utility>

namespace plexus {

// Lifts a single-transport Policy P onto the type-erased multiplexer channel:
// a copy of P with only byte_channel_type swapped to polymorphic_byte_channel.
template<typename P>
struct muxify
{
    using executor_type     = typename P::executor_type;
    using byte_channel_type = io::polymorphic_byte_channel;
    using timer_type        = typename P::timer_type;
    using byte_owner        = typename P::byte_owner;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        P::post(ex, std::move(fn));
    }
};

}

#endif

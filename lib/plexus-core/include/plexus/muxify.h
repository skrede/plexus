#ifndef HPP_GUARD_PLEXUS_MUXIFY_H
#define HPP_GUARD_PLEXUS_MUXIFY_H

#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <utility>

namespace plexus {

// Lifts a single-transport Policy P onto the type-erased multiplexer channel: a
// member-for-member copy of P (executor, timer, byte_owner, post) with only the
// byte_channel swapped to io::polymorphic_byte_channel. A node binds muxify<P>
// when it drives more than one wire transport; a single-transport node keeps P
// (the concrete channel, zero indirection).
template <typename P>
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

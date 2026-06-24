#ifndef HPP_GUARD_PLEXUS_POLICY_H
#define HPP_GUARD_PLEXUS_POLICY_H

#include "plexus/detail/compat.h"

#include "plexus/io/byte_channel.h"

#include <chrono>
#include <concepts>
#include <system_error>

namespace plexus {

// expires_after has no return constraint: a steady-timer backend may return the
// count of cancelled waits.
template<typename T>
concept timer = requires(T &t, std::chrono::milliseconds dur, detail::move_only_function<void(std::error_code)> handler) {
    t.expires_after(dur);
    { t.async_wait(std::move(handler)) } -> std::same_as<void>;
    { t.cancel() } -> std::same_as<void>;
};

// Channel construction is deliberately NOT a Policy constraint: it is transport-
// specific and owned by transport_backend; requiring executor-alone construction
// would force a sentinel/setter mutation on any channel needing more.
template<typename P>
concept Policy =
        requires {
            typename P::executor_type;
            typename P::byte_channel_type;
            typename P::timer_type;
            typename P::byte_owner;
        } && io::byte_channel<typename P::byte_channel_type> && timer<typename P::timer_type> && std::constructible_from<typename P::timer_type, typename P::executor_type> &&
        std::constructible_from<typename P::timer_type, typename P::executor_type, std::error_code &> &&
        requires(typename P::executor_type ex, detail::move_only_function<void()> fn) { P::post(ex, std::move(fn)); };

}

#endif

#ifndef HPP_GUARD_PLEXUS_POLICY_H
#define HPP_GUARD_PLEXUS_POLICY_H

#include "plexus/io/byte_channel.h"
#include "plexus/detail/compat.h"

#include <chrono>
#include <concepts>
#include <system_error>

namespace plexus {

// timer<T>: the backend-independent timer surface a Policy supplies. Mirrors the
// mdnspp TimerLike shape — expires_after has no return constraint because a
// steady-timer backend may return the count of cancelled waits.
template<typename T>
concept timer = requires(T &t, std::chrono::milliseconds dur, detail::move_only_function<void(std::error_code)> handler) {
    t.expires_after(dur);
    { t.async_wait(std::move(handler)) } -> std::same_as<void>;
    { t.cancel() } -> std::same_as<void>;
};

// Policy<P>: the single compile-time seam the engine is written against. A Policy
// bundles the hot-path substrate — an executor with a byte_channel and a timer
// (the timer is constructible from the executor, per backend convention) and the
// byte_owner the receive seam binds wire_bytes views to — plus a static post()
// that schedules work onto the executor.
//
// Channel construction is deliberately NOT a Policy constraint: it is irreducibly
// transport-specific (a live socket / a bus handle / a TLS context / a hardening
// config), and is owned by the transport_backend concept
// (dial -> on_dialed(unique_ptr<channel>), listen -> on_accepted(...)). Requiring
// executor-alone channel constructibility would force a sentinel/setter mutation
// anti-pattern on any channel that needs more than the executor; construction
// belongs to the transport, not the Policy. (asio_channel/inproc_channel still
// OFFER executor-alone construction as a convenience — a removed requirement is
// not a ban.)
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

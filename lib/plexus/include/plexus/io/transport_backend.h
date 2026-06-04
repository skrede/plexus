#ifndef HPP_GUARD_PLEXUS_IO_TRANSPORT_BACKEND_H
#define HPP_GUARD_PLEXUS_IO_TRANSPORT_BACKEND_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include "plexus/policy.h"

#include <memory>
#include <concepts>

namespace plexus::io {

// Concept admitting every per-backend connector the engine drives to obtain a
// live byte_channel without knowing the backend. Compile-time, header-only, kept
// OUT of Policy (the connector is a cold-path service, not the hot-path
// substrate) and NOT virtual (the MCU profile carries no dispatch).
//
// listen(ep) registers an accepting endpoint; accepted channels arrive via
// on_accepted as a unique_ptr<channel> the caller owns. dial(ep) connects
// asynchronously; on success the live channel is delivered through on_dialed, on
// failure on_dial_failed fires with a mapped io_error. close() stops listening /
// cancels pending work. Both backends present the identical shape so the generic
// engine doesn't care which one it drives.
//
// The callback members are bare-call expressions (no `-> std::same_as`), the void
// verbs are constrained — the same split byte_channel uses.
template <typename T, typename Policy>
concept transport_backend = plexus::Policy<Policy> && requires(T &t,
                                const io::endpoint &ep,
                                detail::move_only_function<void(std::unique_ptr<typename Policy::byte_channel_type>)> on_ch,
                                detail::move_only_function<void(io_error)> on_err)
{
    { t.listen(ep) }                    -> std::same_as<void>;
    t.on_accepted(std::move(on_ch));
    { t.dial(ep) }                      -> std::same_as<void>;
    t.on_dialed(std::move(on_ch));
    t.on_dial_failed(std::move(on_err));
    t.on_error(std::move(on_err));
    { t.close() }                       -> std::same_as<void>;
};

}

#endif

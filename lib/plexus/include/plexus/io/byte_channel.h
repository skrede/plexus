#ifndef HPP_GUARD_PLEXUS_IO_BYTE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_BYTE_CHANNEL_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <concepts>

namespace plexus::io {

// Concept admitting every per-connection byte-stream type the plexus io layer
// drives: the backend-independent surface both the inproc and TCP channels
// satisfy. send() + the on_* handler registration subsumes the underlying
// async read / async write.
//
// Two deliberate departures from the precedent socket concept:
//   * No executor-affinity member. Binding the channel to a backend executor
//     handle welds the concept to one executor and one backend; demand for
//     synchronization is expressed through the Policy's executor, not leaked
//     into the io surface.
//   * on_data delivery is ALWAYS posted onto the executor, never invoked
//     synchronously from inside send()/feed(). Consumers may therefore call back
//     into the channel from a handler without reentrancy hazards.
//
// Handlers are plexus::detail::move_only_function so move-only callables are
// admissible (no copyable-callable constraint).
template <typename C>
concept byte_channel = requires(C &c,
                                const C &cc,
                                std::span<const std::byte> bytes,
                                detail::move_only_function<void(std::span<const std::byte>)> on_data_cb,
                                detail::move_only_function<void()> on_closed_cb,
                                detail::move_only_function<void(io_error)> on_error_cb)
{
    { c.send(bytes) }                          -> std::same_as<void>;
    { c.close() }                              -> std::same_as<void>;
    { cc.remote_endpoint() }                   -> std::same_as<endpoint>;
    c.on_data(std::move(on_data_cb));
    c.on_closed(std::move(on_closed_cb));
    c.on_error(std::move(on_error_cb));
};

}

#endif

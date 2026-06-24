#ifndef HPP_GUARD_PLEXUS_IO_TRANSPORT_BACKEND_H
#define HPP_GUARD_PLEXUS_IO_TRANSPORT_BACKEND_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <memory>
#include <concepts>

namespace plexus::io {

// on_dialed/on_dial_failed carry the dialed endpoint as the correlation key: a node
// driving many concurrent dials over ONE transport routes each completion back to its
// slot by endpoint, since a real async transport completes dials out of order.
// on_accepted carries no endpoint (an inbound channel is not correlated to a dial).
template<typename T, typename Policy>
concept transport_backend = plexus::Policy<Policy> &&
        requires(T &t, const io::endpoint &ep, plexus::detail::move_only_function<void(std::unique_ptr<typename Policy::byte_channel_type>)> on_acc,
                 plexus::detail::move_only_function<void(std::unique_ptr<typename Policy::byte_channel_type>, const io::endpoint &)> on_ch,
                 plexus::detail::move_only_function<void(const io::endpoint &, io_error)> on_dfail, plexus::detail::move_only_function<void(io_error)> on_err) {
            { t.listen(ep) } -> std::same_as<void>;
            t.on_accepted(std::move(on_acc));
            { t.dial(ep) } -> std::same_as<void>;
            t.on_dialed(std::move(on_ch));
            t.on_dial_failed(std::move(on_dfail));
            t.on_error(std::move(on_err));
            { t.close() } -> std::same_as<void>;
        };

}

#endif

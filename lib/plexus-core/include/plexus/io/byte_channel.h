#ifndef HPP_GUARD_PLEXUS_IO_BYTE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_BYTE_CHANNEL_H

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/wire/close_cause.h"

#include <span>
#include <cstddef>
#include <concepts>

namespace plexus::io {

// Contract every backend honors identically: on_data is ALWAYS posted onto the
// executor (never invoked synchronously from send()/feed()), so a handler may call
// back into the channel without reentrancy hazard; the delivered span is a COMPLETE
// header-on frame (a frame_header followed by its payload), NOT the stripped inner
// payload. on_error and on_protocol_close are DISTINCT seams: on_error is a network
// drop (re-dials); on_protocol_close is a wire-misbehaving peer (does NOT re-dial).
template<typename C>
concept byte_channel = requires(C &c, const C &cc, std::span<const std::byte> bytes, plexus::detail::move_only_function<void(std::span<const std::byte>)> on_data_cb,
                                plexus::detail::move_only_function<void()> on_closed_cb, plexus::detail::move_only_function<void(io_error)> on_error_cb,
                                plexus::detail::move_only_function<void(wire::close_cause)> on_protocol_close_cb) {
    { c.send(bytes) } -> std::same_as<void>;
    { c.close() } -> std::same_as<void>;
    { cc.remote_endpoint() } -> std::same_as<endpoint>;
    c.on_data(std::move(on_data_cb));
    c.on_closed(std::move(on_closed_cb));
    c.on_error(std::move(on_error_cb));
    c.on_protocol_close(std::move(on_protocol_close_cb));
};

}

#endif

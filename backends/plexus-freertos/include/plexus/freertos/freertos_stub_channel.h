#ifndef HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_STUB_CHANNEL_H
#define HPP_GUARD_PLEXUS_FREERTOS_FREERTOS_STUB_CHANNEL_H

#include "plexus/io/byte_channel.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>

namespace plexus::freertos {

// A no-op byte_channel for the constrained-target substrate: every member the
// byte_channel concept names, all doing nothing. The substrate is deliberately
// transport-free at this stage — the byte_channel_type slot of a Policy need only
// SATISFY the byte_channel concept, never be constructed (channel construction
// belongs to the transport, not the Policy). Binding the slot to a real serial /
// lwIP channel would pull a whole transport into the substrate prematurely; the
// stub keeps the seam compiling while the real channel lands later.
//
// The in-header static_assert below makes a non-conforming stub fail with a single
// concept diagnostic at this source, not a template-instantiation dump downstream.
class freertos_stub_channel
{
public:
    freertos_stub_channel() = default;

    void send(std::span<const std::byte>)
    {
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }
};

static_assert(plexus::io::byte_channel<freertos_stub_channel>, "freertos_stub_channel must satisfy byte_channel — check every on_* member and send/close");

}

#endif

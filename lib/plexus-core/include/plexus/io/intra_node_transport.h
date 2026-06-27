#ifndef HPP_GUARD_PLEXUS_IO_INTRA_NODE_TRANSPORT_H
#define HPP_GUARD_PLEXUS_IO_INTRA_NODE_TRANSPORT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/process_loopback_channel.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <memory>
#include <utility>
#include <string_view>

namespace plexus::io {

// The top-tier carrier in the locality-priority selection: a node's OWN subscribers. It satisfies
// both transport_backend (single-transport, the pure zero-erasure case) and mux_member (composed
// with a network transport). All backend verbs are INERT: the self-channel is pre-connected and
// attached via the forwarder's attach_local, never dialed or accepted — so listen/dial/accept
// completions never fire. mux_tier is `local` (the routing axis has no `process`); the channel's
// scheme "inproc" carries the delivery-tier classification (process) the forwarder fast-paths on.
template<typename Policy>
class intra_node_transport
{
public:
    using channel_type = process_loopback_channel<Policy>;

    static constexpr std::array<std::string_view, 1> k_schemes{"inproc"};
    static constexpr std::span<const std::string_view> mux_schemes{k_schemes};
    static constexpr transport_kind mux_tier = transport_kind::local;

    // Ranks the intra-node member first within the mux local tier (read by member_prefers_local_fast).
    // "inproc" collides with no other member's scheme, so this flag plus the pack-order convention is
    // the whole of the locality-priority ranking — no selector sub-ordering hook is required.
    static constexpr bool mux_prefers_local_fast = true;

    void listen(const endpoint &)
    {
    }
    void dial(const endpoint &)
    {
    }
    void close()
    {
    }

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_on_accepted_cb = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> cb)
    {
        m_on_dialed_cb = std::move(cb);
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb)
    {
        m_on_dial_failed_cb = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

private:
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> m_on_accepted_cb;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> m_on_dialed_cb;
    plexus::detail::move_only_function<void(const endpoint &, io_error)> m_on_dial_failed_cb;
    plexus::detail::move_only_function<void(io_error)> m_on_error_cb;
};

}

#endif

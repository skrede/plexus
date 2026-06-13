#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_INTERNALS_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_INTERNALS_H

#include "plexus/io/endpoint.h"
#include "plexus/io/peer_observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::detail {

// The object-lane dispatch entry a typed subscriber registers ALONGSIDE its bytes
// adapter (under the one registration id). native_key is the process-local C++ type
// witness (the address of a per-T inline constant); dispatch recovers the concrete T
// from the carrier's slot and invokes the typed callback. A NULL native_key marks a
// bytes-only subscription with no object entry.
struct object_entry
{
    const void *native_key{};
    plexus::detail::move_only_function<void(const io::object_carrier &, const io::message_info &)> dispatch;
};

struct subscription
{
    std::string        fqn;
    io::subscriber_qos qos;
    // The subscriber-declared type identity (std::nullopt = undeclared), stored so a
    // late-discovered peer gets the typed demand fanned with the gate intact.
    std::optional<std::uint64_t> type_id;
    plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb;
    object_entry obj{};
};

// The private observer the node registers on its own engine: a peer-ready edge
// re-fans every standing demand toward that peer (idempotent — remember_demand
// dedups per (peer, fqn)). The fan-out runs POSTED on the borrowed executor, so this
// bookkeeping needs no locking. node_id is recovered from the node_name key the edge
// carries; an unparsable name is skipped (it never matched a known peer anyway).
template <typename Node>
struct peer_watch : io::peer_observer
{
    Node &owner;
    explicit peer_watch(Node &n) : owner(n) {}
    void on_peer_ready(const plexus::node_id &id, std::string_view, io::peer_kind) override
    {
        owner.note_known_peer(id);
        owner.fan_demands_to(id);
    }
};

}

#endif

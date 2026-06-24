#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_INTERNALS_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_INTERNALS_H

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::detail {

// native_key is the process-local C++ type witness (the address of a per-T inline constant); a null
// native_key marks a bytes-only subscription with no object entry.
struct object_entry
{
    const void *native_key{};
    plexus::detail::move_only_function<void(const io::object_carrier &, const io::message_info &)> dispatch;
};

struct subscription
{
    std::string fqn;
    io::subscriber_qos qos;
    // std::nullopt = undeclared type, stored so a late-discovered peer gets the typed demand fanned
    // with the gate intact.
    std::optional<std::uint64_t> type_id;
    plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)> cb;
    object_entry obj{};
};

// A peer-ready edge re-fans every standing demand toward that peer; remember_demand dedups per
// (peer, fqn), so the re-fan is idempotent. The fan-out runs posted on the borrowed executor, so
// this bookkeeping needs no locking.
template<typename Node>
struct peer_watch : io::observer
{
    Node &owner;
    explicit peer_watch(Node &n)
            : owner(n)
    {
    }

    void on_peer_ready(const plexus::node_id &id, std::string_view, io::peer_kind) override
    {
        owner.note_known_peer(id);
        owner.fan_demands_to(id);
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SLOT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SLOT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/session_build_context.h"

#include "plexus/io/detail/channel_graveyard.h"

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io::detail {

// The member order encodes the destruction discipline: session is destroyed first, so the record (whose channel/epoch
// it borrows), the retired cell and the driver outlive every incarnation. A rebuild waits in parked until the old
// session's waits have completed and retired is empty. A denied or closing slot retires instead of building, and runs
// on_emptied_cb once it holds nothing and its driver has settled.
template<typename Policy, typename Transport, typename Clock>
struct session_slot
{
    using policy_type  = Policy;
    using channel_type = typename Policy::byte_channel_type;

    session_slot(Transport &transport, session_build_context<Policy> &build, const node_id &id, const endpoint &ep, const std::string &node_name)
            : record()
            , retired()
            , driver(transport, build.executor, build.redial, ep, build.redial_seed)
            , parked()
            , session()
            , dead(false)
            , on_emptied_cb()
    {
        record.peer_id       = id;
        record.node_name     = node_name;
        record.dial_endpoint = ep;
    }

    peer_context<Policy> record;
    channel_graveyard<channel_type, 1> retired;
    reconnect<Policy, Transport, Clock> driver;
    std::unique_ptr<channel_type> parked;
    std::optional<peer_session<Policy>> session;
    bool dead;
    plexus::detail::move_only_function<void()> on_emptied_cb;
};

inline node_id inbound_slot_id(std::uint64_t n)
{
    node_id id{};
    for(std::size_t i = 0; i < 8; ++i)
        id[15 - i] = std::byte{static_cast<std::uint8_t>(n >> (8 * i))};
    return id;
}

}

#endif

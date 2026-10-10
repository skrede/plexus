#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SLOT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_SLOT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/session_build_context.h"

#include "plexus/node_id.h"

#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io::detail {

// The member order encodes the destruction discipline: session is destroyed first, so the
// record (whose channel/epoch it borrows) and the driver outlive every incarnation.
template<typename Policy, typename Transport, typename Clock>
struct session_slot
{
    session_slot(Transport &transport, session_build_context<Policy> &build, const node_id &id, const endpoint &ep, const std::string &node_name)
            : driver(transport, build.executor, build.redial, ep, build.redial_seed)
            , dead(false)
    {
        record.peer_id       = id;
        record.node_name     = node_name;
        record.dial_endpoint = ep;
    }

    peer_context<Policy> record;
    reconnect<Policy, Transport, Clock> driver;
    std::optional<peer_session<Policy>> session;
    bool dead;
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

#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_OBJECT_DISPATCH_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_OBJECT_DISPATCH_H

#include "plexus/detail/node_internals.h"
#include "plexus/detail/node_subscriptions.h"

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"

#include "plexus/wire/frame_codec.h"

#include <cstdint>
#include <string_view>

namespace plexus::detail {

// reception / reception_read are threaded by reference so the receive clock is read once on
// first need and shared across the fan; a subscriber wanting no info gets a 0 stamp.
inline io::message_info object_info_for(const subscription &sub, const io::object_carrier &carrier, std::uint64_t &reception, bool &reception_read)
{
    io::message_info info{};
    info.publication_sequence = carrier.sequence;
    info.from_intra_process   = true;
    if(sub.qos.wants_message_info)
    {
        if(!reception_read)
        {
            reception      = wire::now_timestamp_ns();
            reception_read = true;
        }
        info.source_timestamp    = carrier.source_timestamp;
        info.reception_timestamp = reception;
    }
    return info;
}

// A native_key MATCH dispatches the concrete T for the callback's duration only (the session
// releases the slot right after); a MISMATCH is counted and warn-and-dropped, never a cast.
template<typename Node>
void dispatch_object(Node &n, std::string_view fqn, const io::object_carrier &carrier)
{
    subscription_table::dispatch_guard guard{n.m_subscriptions};

    // The receive clock is read at most once, and only if a matching subscriber wants
    // message_info.
    std::uint64_t reception = 0;
    bool reception_read     = false;

    for(auto &[rid, sub] : n.m_subscriptions.entries())
    {
        if(sub.fqn != fqn || sub.obj.native_key == nullptr || n.m_subscriptions.pending_removed(rid))
            continue;
        if(sub.obj.native_key != carrier.native_key)
        {
            ++n.m_object_dispatch_mismatch;
            n.m_logger.warn("plexus: node object_native_key_mismatch");
            continue;
        }
        sub.obj.dispatch(carrier, object_info_for(sub, carrier, reception, reception_read));
    }
}

}

#endif

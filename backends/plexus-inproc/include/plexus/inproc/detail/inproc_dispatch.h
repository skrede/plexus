#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H

#include "plexus/io/object_carrier.h"

#include <span>
#include <cstddef>

// These live in plexus::detail (NOT a new plexus::inproc::detail namespace): a sibling inproc
// detail namespace would shadow the bare detail::move_only_function lookups inproc_bus.h and
// inproc_executor.h resolve to plexus::detail. The reach is by namespace fall-through.
namespace plexus::detail {

// Deliver one dequeued packet to its keyed target and balance the bus's reference, relocated out
// of inproc_bus::deliver_one (a friend, so it reaches the channel table + the per-packet release/
// report tails through the bus reference). A close/object/data packet routes to the matching
// channel's deliver_*; an object whose target vanished still releases (no leak); an unmatched data
// packet reports unroutable on a still-registered sender (already off the synchronous send path).
template<typename Bus, typename Packet>
void dispatch_packet(Bus &bus, Packet &pkt)
{
    using packet_kind = typename Bus::packet_kind;
    bool delivered    = false;
    for(auto &entry : bus.m_channels)
        if(pkt.to_key == entry.key)
        {
            if(pkt.kind == packet_kind::close)
                entry.chan->deliver_close();
            else if(pkt.kind == packet_kind::object)
                entry.chan->deliver_object(pkt.carrier); // the channel releases on its own path
            else
                entry.chan->deliver(std::span<const std::byte>(pkt.data));
            delivered = true;
            break;
        }

    if(pkt.kind == packet_kind::object && !delivered)
        io::release(pkt.carrier);
    if(pkt.kind == packet_kind::data && !delivered && pkt.from && bus.sender_live(pkt.from))
        pkt.from->report_unroutable();
    pkt.carrier = {}; // drop the dangling slot pointer so a re-read never re-releases
    pkt.from    = nullptr;
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H

#include "plexus/io/object_carrier.h"

#include <span>
#include <cstddef>

// These live in plexus::detail, NOT a new plexus::inproc::detail: a sibling inproc detail namespace
// would shadow the bare detail::move_only_function lookups inproc_bus.h / inproc_executor.h resolve
// to plexus::detail. The reach is by namespace fall-through.
namespace plexus::detail {

// An object whose target vanished still releases (no leak); an unmatched data packet reports
// unroutable on a still-registered sender (already off the synchronous send path).
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
                entry.chan->deliver_object(pkt.carrier);
            else
                entry.chan->deliver(std::span<const std::byte>(pkt.data));
            delivered = true;
            break;
        }

    if(pkt.kind == packet_kind::object && !delivered)
        io::release(pkt.carrier);
    if(pkt.kind == packet_kind::data && !delivered && pkt.from && bus.sender_live(pkt.from))
        pkt.from->report_unroutable();
    // drop the dangling slot pointer so a re-read never re-releases
    pkt.carrier = {};
    pkt.from    = nullptr;
}

}

#endif

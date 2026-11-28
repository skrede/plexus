#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_DISPATCH_H

#include "plexus/io/object_carrier.h"

#include <span>
#include <cstddef>

// These live in plexus::detail, NOT a new plexus::inproc::detail: a sibling inproc detail namespace
// would shadow the bare detail::move_only_function lookups inproc_bus.h / inproc_executor.h resolve
// to plexus::detail. The reach is by namespace fall-through.
namespace plexus::detail {

template<typename Channels, typename Packet>
bool deliver_to_target(Channels &channels, Packet &pkt)
{
    using packet_kind = decltype(Packet::kind);
    for(auto &entry : channels)
        if(pkt.to_key == entry.key)
        {
            if(pkt.kind == packet_kind::close)
                entry.chan->deliver_close();
            else if(pkt.kind == packet_kind::object)
                entry.chan->deliver_object(pkt.carrier);
            else
                entry.chan->deliver(std::span<const std::byte>(pkt.data));
            return true;
        }
    return false;
}

// An object whose target vanished still releases (no leak); an unmatched data packet reports
// unroutable to its sender if that is still registered (already off the synchronous send path).
template<typename Bus, typename Packet>
void dispatch_packet(Bus &bus, Packet &pkt)
{
    using packet_kind    = decltype(Packet::kind);
    const bool delivered = deliver_to_target(bus.m_channels, pkt);
    if(pkt.kind == packet_kind::object && !delivered)
        io::release(pkt.carrier);
    if(pkt.kind == packet_kind::data && !delivered && pkt.from_key != 0)
        bus.report_unroutable(pkt.from_key);
}

}

#endif

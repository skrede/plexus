#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_INBOUND_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_INBOUND_H

#include "plexus/discovery/detail/announcement_card.h"

#include "plexus/match/key_pattern.h"

#include "plexus/wire/announcement.h"

#include <span>
#include <string>
#include <cstddef>

namespace plexus::discovery::detail {

// The concrete/concrete common case takes the flagless uint32 fast-path (no parse, no alloc);
// otherwise the universe patterns must intersect — a rider on the shared matcher, never a second
// glob. FNV(factory/line/*) != FNV(factory/line/1) yet they intersect, so a flagged peer must never
// take the fast-path. A malformed peer pattern or an unparsable local pattern fails closed.
template<typename D>
bool universe_admits(const D &d, const wire::announcement &ann)
{
    if(d.m_universe_is_concrete && !(ann.flags & wire::k_announcement_universe_pattern_flag))
        return ann.universe == d.m_options.universe;
    const auto peer = match::key_pattern::make(ann.universe_pattern);
    return d.m_universe_pattern && peer && d.m_universe_pattern->intersects(*peer);
}

template<typename Clock, typename D>
void dispatch_announcement(D &d, const wire::announcement &ann, const std::string &source)
{
    auto info = detail::service_info_from_announcement(ann, source);
    if((ann.flags & wire::k_announcement_goodbye_flag) != 0)
    {
        // A goodbye is a removal: never rate-limit or cap it, a leaver must always be able to leave.
        if(d.m_on_withdrawn_cb)
            d.m_on_withdrawn_cb(info);
        return;
    }
    if(d.m_on_resolved_cb && d.m_flood_cap.admit(source, Clock::now()))
        d.m_on_resolved_cb(info);
}

template<typename Clock, typename D, typename Endpoint>
void on_inbound(D &d, const Endpoint &from, std::span<const std::byte> bytes)
{
    const auto ann = wire::decode_announcement(bytes);
    if(!ann)
        return;
    // Fail-closed ahead of self-echo, goodbye, and admission: a foreign universe is dropped whole, evicting no peer and paying no admission or alloc.
    if(!universe_admits(d, *ann))
        return;
    // A node never notes itself: its own echo (same node_id) is recorded for the self-probe then dropped, keeping awareness cross-node.
    if(d.m_announcement && ann->node_id == d.m_announcement->node_id)
    {
        d.m_self_seen = true;
        return;
    }
    dispatch_announcement<Clock>(d, *ann, from.address().to_string());
}

}

#endif

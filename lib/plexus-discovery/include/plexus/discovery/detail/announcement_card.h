#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_ANNOUNCEMENT_CARD_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_ANNOUNCEMENT_CARD_H

#include "plexus/wire/announcement.h"

#include "plexus/io/endpoint.h"
#include "plexus/node_id.h"
#include "plexus/discovery/universe.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

namespace plexus::discovery::detail {

inline wire::announcement announcement_from_card(const node_id &id, const std::vector<::plexus::discovery::listening_transport> &listens, std::uint64_t ttl_secs, std::uint8_t flags, std::uint32_t universe = k_default_universe, std::string_view universe_pattern = k_default_universe_label)
{
    wire::announcement ann;
    ann.flags    = flags;
    ann.universe = universe;
    ann.node_id  = id;
    ann.ttl_secs = ttl_secs;
    // The presence flag and pattern bytes are keyed to the LABEL, never the uint32: a node whose
    // effective label is the default emits flagless with no pattern bytes (byte-identical legacy wire)
    // even if its uint32 was set non-default, so a stamped announcement never carries a flagged stale
    // default that two different-universe nodes would then intersect-match and mutually admit.
    if(universe_pattern != k_default_universe_label)
    {
        ann.universe_pattern.assign(universe_pattern);
        ann.flags |= wire::k_announcement_universe_pattern_flag;
    }
    ann.listens.reserve(listens.size());
    for(const auto &t : listens)
        ann.listens.emplace_back(t.transport, t.port);
    return ann;
}

// Build a service_info whose metadata is BYTE-IDENTICAL to assemble_contact_card by routing the
// announcement's listens back through it (never hand-rolled keys), so the node's untouched
// note_from_card reads it. source_host is the datagram's kernel source IP, never a wire field.
inline ::plexus::discovery::service_info service_info_from_announcement(const wire::announcement &ann, const std::string &source_host)
{
    std::vector<::plexus::discovery::listening_transport> listens;
    listens.reserve(ann.listens.size());
    for(const auto &[transport, port] : ann.listens)
        listens.push_back({transport, port});
    return ::plexus::discovery::service_info{::plexus::discovery::detail::hex_encode(ann.node_id), io::endpoint{"", source_host}, ::plexus::discovery::assemble_contact_card(ann.node_id, listens)};
}

}

#endif

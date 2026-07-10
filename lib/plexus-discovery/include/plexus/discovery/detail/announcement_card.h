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

namespace plexus::discovery::detail {

inline wire::announcement announcement_from_card(const node_id &id, const std::vector<::plexus::discovery::listening_transport> &listens, std::uint64_t ttl_secs, std::uint8_t flags, std::uint32_t universe = k_default_universe)
{
    wire::announcement ann;
    ann.flags    = flags;
    ann.universe = universe;
    ann.node_id  = id;
    ann.ttl_secs = ttl_secs;
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

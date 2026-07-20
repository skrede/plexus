#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_CARD_CODEC_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_CARD_CODEC_H

#include "plexus/discovery/detail/announcement_card.h"

#include "plexus/wire/announcement.h"

#include "plexus/node_id.h"
#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::discovery::detail {

inline constexpr std::string_view k_port_key_prefix = "plexus/";
inline constexpr std::string_view k_port_key_suffix = "/port";

// Recover the (transport, port) listens from an assembled card's "plexus/<transport>/port" keys,
// reusing read_transport_port so the parse matches the node's. The node_id and schema keys are
// skipped (no /port suffix). This is the inverse of advertise's assemble_contact_card.
inline std::vector<::plexus::discovery::listening_transport> listens_from_card(const std::vector<std::pair<std::string, std::string>> &card)
{
    std::vector<::plexus::discovery::listening_transport> listens;
    for(const auto &[key, value] : card)
    {
        if(key.size() <= k_port_key_prefix.size() + k_port_key_suffix.size())
            continue;
        if(key.compare(0, k_port_key_prefix.size(), k_port_key_prefix) != 0 || key.compare(key.size() - k_port_key_suffix.size(), k_port_key_suffix.size(), k_port_key_suffix) != 0)
            continue;
        const std::string transport = key.substr(k_port_key_prefix.size(), key.size() - k_port_key_prefix.size() - k_port_key_suffix.size());
        if(const auto port = ::plexus::discovery::read_transport_port(card, transport))
            listens.push_back({transport, *port});
    }
    return listens;
}

// Build the wire announcement for a card once (at advertise time), so the per-emit path only
// re-encodes it into the reused scratch and never rebuilds the listens. nullopt for a card with
// an unparsable node_id key (nothing to advertise).
inline std::optional<wire::announcement> announcement_from_service_info(const ::plexus::discovery::service_info &card, std::uint64_t ttl_secs, std::uint32_t universe, std::string_view universe_pattern = k_default_universe_label)
{
    const auto id = ::plexus::discovery::detail::hex_decode(card.metadata.empty() ? std::string_view{} : std::string_view{card.metadata.front().second});
    if(!id)
        return std::nullopt;
    return announcement_from_card(*id, listens_from_card(card.metadata), ttl_secs, 0, universe, universe_pattern);
}

}

#endif

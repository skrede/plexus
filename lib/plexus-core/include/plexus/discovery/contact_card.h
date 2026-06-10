#ifndef HPP_GUARD_PLEXUS_DISCOVERY_CONTACT_CARD_H
#define HPP_GUARD_PLEXUS_DISCOVERY_CONTACT_CARD_H

#include "plexus/node_id.h"

#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <charconv>

namespace plexus::discovery {

// The contact card is a node's complete advertised identity: its authenticated
// node_id, one reachable port per listening transport, and a schema version — and
// NOTHING else. There is deliberately no parameter for topic, type, publisher, or
// security data, so the broadcast structurally cannot carry it (a node advertising
// topic/posture state would invite the eager-discovery staleness pathology and leak
// the key realm on an unauthenticated link); topic identity is resolved in-band.
//
// The schema-version key is the only forward-compatibility surface: a browser
// ignores keys it does not know, and the version lets the card schema evolve.

inline constexpr std::uint32_t k_contact_card_schema_version = 1;

inline constexpr std::string_view k_card_node_id_key = "node_id";
inline constexpr std::string_view k_card_schema_key = "plexus/schema";

struct listening_transport
{
    std::string transport;
    std::uint16_t port;
};

namespace detail {

inline std::string hex_encode(const node_id &id)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string out;
    out.reserve(id.size() * 2);
    for(const std::byte b : id)
    {
        const auto v = static_cast<unsigned>(b);
        out.push_back(digits[(v >> 4) & 0x0f]);
        out.push_back(digits[v & 0x0f]);
    }
    return out;
}

inline std::string port_key(std::string_view transport)
{
    std::string key = "plexus/";
    key += transport;
    key += "/port";
    return key;
}

}

// Assemble the card for one node (one record per node): the authenticated node_id,
// a "plexus/<transport>/port" key per listening transport, and the schema version.
[[nodiscard]] inline std::vector<std::pair<std::string, std::string>>
assemble_contact_card(const node_id &id, const std::vector<listening_transport> &transports)
{
    std::vector<std::pair<std::string, std::string>> card;
    card.reserve(transports.size() + 2);
    card.emplace_back(std::string{k_card_node_id_key}, detail::hex_encode(id));
    for(const auto &t : transports)
        card.emplace_back(detail::port_key(t.transport), std::to_string(t.port));
    card.emplace_back(std::string{k_card_schema_key}, std::to_string(k_contact_card_schema_version));
    return card;
}

// Read a "plexus/<transport>/port" value back out of a browsed card so a peer dials
// the advertised endpoint with no hardcoded port. Absent or unparsable yields none.
[[nodiscard]] inline std::optional<std::uint16_t>
read_transport_port(const std::vector<std::pair<std::string, std::string>> &card,
                    std::string_view transport)
{
    const std::string key = detail::port_key(transport);
    for(const auto &[k, v] : card)
    {
        if(k != key)
            continue;
        std::uint16_t port{};
        const char *first = v.data();
        const char *last = v.data() + v.size();
        if(std::from_chars(first, last, port).ec == std::errc{})
            return port;
        return std::nullopt;
    }
    return std::nullopt;
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_TESTS_DISCOVERY_TEST_CONTACT_CARD_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_DISCOVERY_TEST_CONTACT_CARD_COMMON_H

// The contact-card oracle: the node-level discovery card is node_id + one port key
// per listening transport + a schema version, and NOTHING else. It proves the exact
// key set, the structural absence of any topic/type/publisher/posture/key-id key, the
// verbatim carry through static_discovery, that the advertised node_id is the
// authenticated-peer identity (not a self-asserted value), and that a browsing peer
// derives its dial port from the card with no hardcoded port. Header-only core only
// (plexus::plexus) — no backend, no socket, no mDNS.

#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/static_discovery.h"
#include "plexus/io/host_identity.h"
#include "plexus/io/security/attach_facts.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cctype>
#include <string>
#include <vector>
#include <cstddef>
#include <algorithm>

namespace contact_card_fixture {

using plexus::discovery::assemble_contact_card;
using plexus::discovery::read_transport_port;
using plexus::discovery::listening_transport;
using plexus::discovery::service_info;
using plexus::discovery::static_discovery;
using plexus::discovery::detail::hex_encode;
using plexus::discovery::detail::hex_decode;

inline plexus::node_id node_id_of(int seed)
{
    plexus::node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>((seed + static_cast<int>(i)) & 0xff);
    return id;
}

inline bool has_key(const std::vector<std::pair<std::string, std::string>> &card, const std::string &key)
{
    return std::ranges::any_of(card, [&](const auto &kv) { return kv.first == key; });
}

inline std::string value_of(const std::vector<std::pair<std::string, std::string>> &card, const std::string &key)
{
    for(const auto &[k, v] : card)
        if(k == key)
            return v;
    return {};
}

}

#endif

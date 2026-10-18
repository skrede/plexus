#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_CONFIG_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_NODE_CONFIG_H

#include "child_args.h"

#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/reconnect_config.h"

#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace shm_harness {

inline plexus::node_id harness_id(std::uint32_t index)
{
    plexus::node_id id{};
    id[0] = std::byte(0x40 + index);
    return id;
}

inline std::vector<plexus::node_id> harness_ids(std::uint32_t count)
{
    std::vector<plexus::node_id> ids;
    for(std::uint32_t index = 0; index < count; ++index)
        ids.push_back(harness_id(index));
    return ids;
}

// One-way wiring gives only the higher-indexed node of each pair the other's card, so exactly one side dials; crossed
// wiring gives every node every card, so both sides of each pair dial.
inline bool dials(const node_args &args, std::uint32_t peer)
{
    return peer != args.index && (args.crossed || peer < args.index);
}

inline std::vector<plexus::discovery::service_info> peer_cards(const node_args &args)
{
    std::vector<plexus::discovery::service_info> cards;
    for(std::uint32_t peer = 0; peer < args.count; ++peer)
    {
        if(!dials(args, peer))
            continue;
        const auto card = plexus::discovery::assemble_contact_card(harness_id(peer), {{"tcp", args.ports[peer]}});
        cards.push_back({"shm_harness." + std::to_string(peer), plexus::io::endpoint{"", "127.0.0.1"}, card});
    }
    return cards;
}

inline plexus::node_options dial_opts()
{
    using namespace std::chrono_literals;
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{20ms, 500ms, std::nullopt, std::nullopt};
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = true;
    return opts;
}

inline plexus::topic_qos topic_qos_for(bool hint)
{
    plexus::topic_qos qos;
    qos.dispatch = hint ? plexus::io::dispatch_hint::frequent : plexus::io::dispatch_hint::none;
    return qos;
}

inline plexus::io::subscriber_qos subscriber_qos_for(bool hint)
{
    plexus::io::subscriber_qos qos;
    qos.dispatch = hint ? plexus::io::dispatch_hint::frequent : plexus::io::dispatch_hint::none;
    return qos;
}

}

#endif

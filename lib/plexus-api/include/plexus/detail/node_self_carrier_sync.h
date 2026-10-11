#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_SYNC_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_SYNC_H

#include "plexus/detail/node_self_carrier.h"

#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::detail {

// Any live carrier's write half supplies the channel reference the deliver peer needs (the peer is
// never dereferenced); the receive sink only fires while a carrier is live, so the map is non-empty.
template<typename Node>
typename Node::engine_channel &any_self_carrier_channel(Node &n) noexcept
{
    return *n.m_self_carriers.begin()->second.write;
}

// The shm self-ring's read half: strip the frame header off the ring's framed bytes and route the
// inner through the forwarder's decode into the node's own dispatch — bypassing the session-keyed
// inject_upgrade_receive (a node has no session to itself). The frame's write-half channel is the
// (ignored) peer the deliver overload requires.
template<typename Node>
void self_carrier_bytes(Node &n, std::span<const std::byte> framed)
{
    if constexpr(Node::k_has_shm_self_carrier)
    {
        const auto hdr = wire::decode_header(framed);
        if(!hdr)
            return;
        const std::span<const std::byte> inner = framed.subspan(wire::header_size);
        const bool has_source_identity         = (hdr->flags & wire::k_flag_source_identity) != 0;
        io::message_info info{};
        info.from_intra_process = true;
        typename io::message_forwarder<typename Node::engine_policy>::peer self{any_self_carrier_channel(n), n.m_service_name};
        n.m_engine.messages().deliver(self, inner, info, n.m_id, has_source_identity,
                                      [&n](std::string_view fqn, std::span<const std::byte> data, const io::message_info &mi) { n.dispatch_message(fqn, data, mi); });
    }
}

template<typename Node>
void teardown_shm_self_carrier(Node &n, const std::string &fqn)
{
    auto it = n.m_self_carriers.find(fqn);
    if(it == n.m_self_carriers.end())
        return;
    n.m_engine.messages().detach_local(fqn, *it->second.write);
    n.m_self_carriers.erase(it);
}

// Mint the node-local shm self-ring for the fqn, record its write half as the self-route (a
// publish fans into the ring), and bind the read half to re-enter the node's own dispatch. A lost
// ring waiter tears the carrier down; the next pub/sub seam that re-drives the sync mints afresh.
template<typename Node>
void install_shm_self_carrier(Node &n, const std::string &fqn)
{
    const io::subscriber_qos qos          = n.m_subscriptions.qos_for(fqn);
    const std::optional<std::uint64_t> tid = n.m_subscriptions.type_id_for(fqn);
    n.for_each_shm_member(
            [&](auto &m)
            {
                auto deliver = [&n](std::span<const std::byte> framed) { self_carrier_bytes(n, framed); };
                auto carrier = detail::install_self_carrier<typename Node::engine_channel>(m, fqn, deliver, [&n, fqn] { teardown_shm_self_carrier(n, fqn); });
                if(!carrier.write)
                    return;
                n.m_engine.messages().attach_local(fqn, *carrier.write, n.m_service_name, qos, tid);
                n.m_self_carriers.insert_or_assign(fqn, std::move(carrier));
            });
}

// The shm self-ring mints/holds an OS region, so it engages ONLY when a same-node publisher AND
// subscriber coexist for the fqn (true self-traffic) — a node talking to a distinct-host peer
// creates no region. Idempotent: re-driven from all four pub/sub seams to converge on the state.
template<typename Node>
void sync_shm_self_carrier(Node &n, std::string_view fqn)
{
    const std::string key{fqn};
    const bool want = n.m_local_publishers.contains(key) && n.m_subscriptions.any_for(fqn);
    const bool have = n.m_self_carriers.contains(key);
    if(want && !have)
        install_shm_self_carrier(n, key);
    else if(!want && have)
        teardown_shm_self_carrier(n, key);
}

// The graph edge is one per (participant, topic, role), so it outlives every publisher handle
// but the last: the count is what tells a retired second handle from a retired topic.
template<typename Node>
void forget_local_publisher(Node &n, std::string_view fqn)
{
    auto it = n.m_local_publishers.find(std::string{fqn});
    if(it != n.m_local_publishers.end() && --it->second == 0)
    {
        n.m_local_publishers.erase(it);
        n.m_engine.forget_local_topic(fqn, graph::topic_role::publisher);
    }
    if constexpr(Node::k_has_shm_self_carrier)
        sync_shm_self_carrier(n, fqn);
}

template<typename Node>
void note_local_publisher(Node &n, std::string_view fqn, std::string_view type_name, std::optional<std::uint64_t> type_id)
{
    ++n.m_local_publishers[std::string{fqn}];
    n.m_engine.note_local_topic(fqn, type_name, type_id, graph::topic_role::publisher);
    if constexpr(Node::k_has_shm_self_carrier)
        sync_shm_self_carrier(n, fqn);
}

}

#endif

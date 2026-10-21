#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_SYNC_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_SYNC_H

#include "plexus/detail/node_same_host.h"

#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::detail {

// A frame of the node's own ring re-enters the node's own dispatch through the forwarder's decode, bypassing the
// session-keyed inject_upgrade_receive (a node has no session to itself). The topic's lane is the deliver peer's
// channel, never dereferenced; a frame that arrives with no live lane is dropped.
template<typename Node>
void self_carrier_bytes(Node &n, std::string_view fqn, std::span<const std::byte> framed)
{
    if constexpr(Node::k_has_shm_self_carrier)
    {
        typename Node::engine_channel *lane = n.m_engine.coordinator().self_channel(fqn);
        const auto hdr                      = wire::decode_header(framed);
        if(lane == nullptr || !hdr)
            return;
        const std::span<const std::byte> inner = framed.subspan(wire::header_size);
        const bool has_source_identity         = (hdr->flags & wire::k_flag_source_identity) != 0;
        io::message_info info{};
        info.from_intra_process = true;
        typename io::message_forwarder<typename Node::engine_policy>::peer self{*lane, n.m_service_name};
        n.m_engine.messages().deliver(self, inner, info, n.m_id, has_source_identity,
                                      [&n](std::string_view topic, std::span<const std::byte> data, const io::message_info &mi) { n.dispatch_message(topic, data, mi); });
    }
}

// Either order of a medium loss is safe: the coordinator's release already cleared every forwarder record of the lane,
// and this teardown then finds none.
template<typename Node>
void teardown_shm_self_carrier(Node &n, const std::string &fqn)
{
    auto it = n.m_self_carriers.find(fqn);
    if(it == n.m_self_carriers.end())
        return;
    if(typename Node::engine_channel *lane = n.m_engine.coordinator().self_channel(fqn))
        n.m_engine.messages().detach_local(fqn, *lane);
    n.m_self_carriers.erase(it);
    n.m_engine.coordinator().release_self(fqn);
}

// The read half: a consumer registration on the node's own publisher ring, type-erased so the node holds one map value
// type; dropping it clears the sink and releases the ring. It is resumed at its attach, since the node's own subscriber
// receives no wire copy.
template<typename Node>
std::shared_ptr<void> mint_self_reader(Node &n, const std::string &fqn)
{
    std::shared_ptr<void> reader;
    for_each_shm_member(n,
                        [&](auto &m)
                        {
                            auto held = m.mint_receive_companion(fqn, m.self_id(), [&n, fqn] { teardown_shm_self_carrier(n, fqn); });
                            if(held)
                                held->resume([&n, fqn](std::span<const std::byte> framed) { self_carrier_bytes(n, fqn, framed); });
                            reader = std::shared_ptr<void>{std::move(held)};
                        });
    return reader;
}

// The node's own subscriber holds the topic's lane as its self-route and reads the same ring as one more consumer, so
// one write serves the node and every co-host reader. Its latched replay reaches the node alone, on a later executor turn
// as a ring frame would, as one operation of the node. A lost ring tears the carrier down; the next pub/sub seam that
// re-drives the sync installs it.
template<typename Node>
void install_shm_self_carrier(Node &n, const std::string &fqn)
{
    typename Node::engine_channel *lane = n.m_engine.coordinator().hold_self(fqn);
    if(lane == nullptr)
        return;
    std::shared_ptr<void> reader = mint_self_reader(n, fqn);
    if(!reader)
        return n.m_engine.coordinator().release_self(fqn);
    n.m_self_carriers.insert_or_assign(fqn, std::move(reader));
    auto replay = [&n, fqn](std::span<const std::byte> framed)
    {
        if(n.m_ops.settled())
            return;
        n.m_ops.begin();
        Node::policy_type::post(n.m_executor,
                                [&n, fqn, owned = std::vector<std::byte>(framed.begin(), framed.end())]
                                {
                                    n.m_ops.end();
                                    self_carrier_bytes(n, fqn, owned);
                                });
    };
    n.m_engine.messages().attach_local(fqn, *lane, n.m_service_name, n.m_subscriptions.qos_for(fqn), n.m_subscriptions.type_id_for(fqn), replay);
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

// The graph edge is one per (participant, topic, role), so it outlives every publisher handle but the last: the count
// is what tells a retired second handle from a retired topic.
template<typename Node>
void forget_local_publisher(Node &n, std::string_view fqn)
{
    auto it = n.m_local_publishers.find(std::string{fqn});
    if(it != n.m_local_publishers.end() && --it->second == 0)
    {
        n.m_local_publishers.erase(it);
        n.m_engine.forget_local_topic(fqn, graph::topic_role::publisher);
        n.m_engine.coordinator().on_local_publisher(fqn, false);
    }
    if constexpr(Node::k_has_shm_self_carrier)
        sync_shm_self_carrier(n, fqn);
}

template<typename Node>
void note_local_publisher(Node &n, std::string_view fqn, std::string_view type_name, std::optional<std::uint64_t> type_id)
{
    if(++n.m_local_publishers[std::string{fqn}] == 1)
        n.m_engine.coordinator().on_local_publisher(fqn, true);
    n.m_engine.note_local_topic(fqn, type_name, type_id, graph::topic_role::publisher);
    if constexpr(Node::k_has_shm_self_carrier)
        sync_shm_self_carrier(n, fqn);
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_RELAY_DEMAND_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_RELAY_DEMAND_H

#include "plexus/io/demand_transition.h"

#include "plexus/io/detail/forward_splice.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/frame_reassembler.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>

namespace plexus::io::detail {

template<typename Engine>
bool publishes_topic(const Engine &e, const node_id &node, std::string_view fqn)
{
    bool found = false;
    e.m_topics.for_each([&](const graph::topic_record &r)
                        {
                            if(!found && r.node == node && r.role == graph::topic_role::publisher && r.name == fqn)
                                found = true;
                        });
    return found;
}

// A downstream consumer subscribed a topic on the relay (an incoming subscribe folds as demand
// up on the serving side): confine the constrained leg to demanded, in-scope topics by
// subscribing upstream on the session of every directly-attached origin publishing the fqn. An
// out-of-scope topic never propagates, so it never transits the narrow leg. The relay's own
// upstream subscribe folds as demand up on the SUBSCRIBER side and is ignored here.
template<typename Engine>
void relay_propagate_demand(Engine &e, std::string_view fqn, demand_transition dir, demand_role role)
{
    if constexpr(!std::is_same_v<typename Engine::forward_splice_type, null_forward_splice>)
    {
        if(dir != demand_transition::up || role != demand_role::publisher || !e.m_splice.forward_scope_admits(fqn))
            return;
        e.m_registry.for_each_connected([&](const node_id &, typename Engine::session_type &s)
                                        {
                                            if(s.is_complete() && publishes_topic(e, s.peer_identity(), fqn))
                                                s.subscribe(fqn);
                                        });
    }
}

// Bind the forwarded-receive re-fan seam to the relay twin. A non-relay engine threads the null twin,
// so the hook is never installed and the forwarded receive stays a pure local delivery — no splice
// pool, no envelope encode, no forwarding-send code reached.
template<typename Engine>
void install_splice_refan(Engine &e)
{
    if constexpr(!std::is_same_v<typename Engine::forward_splice_type, null_forward_splice>)
    {
        e.m_messages.on_forward_refan([&e](std::uint64_t hash, const node_id &origin, std::uint8_t hop, std::span<const std::byte> inner, const typename Engine::channel_type *arrival,
                                           const wire::shared_bytes *owner) { e.m_splice.refan(e.m_messages, hash, origin, hop, inner, arrival, owner); });
        e.m_messages.on_demand_transition([&e](std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
                                          {
                                              e.m_coordinator.on_edge(node_name, fqn, dir, role);
                                              relay_propagate_demand(e, fqn, dir, role);
                                          });
        // The re-origination gate: forward_scope_admits (the engine matcher, reachable only here) AND
        // self-route-aware non-local demand, then reuse refan -> fan_forwarded_buffer (hop 0 -> out_hop
        // 1, per-relay seq, arrival-excluded) and the receive-edge (origin, arrival-relay) dedup.
        e.m_messages.on_originate([&e](std::uint64_t hash, const node_id &origin, std::span<const std::byte> inner, const typename Engine::channel_type *arrival)
                                  {
                                      const std::string_view fqn = e.m_messages.fqn_for(hash);
                                      if(fqn.empty() || !e.m_splice.forward_scope_admits(fqn) || !e.m_messages.has_remote_demand(hash, static_cast<const void *>(arrival)))
                                          return;
                                      e.m_splice.refan(e.m_messages, hash, origin, 0, inner, arrival, nullptr);
                                  });
    }
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINK_INSTALL_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINK_INSTALL_H

#include "plexus/io/locality.h"
#include "plexus/io/observer.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/io/detail/routing_sinks.h"
#include "plexus/io/detail/routing_dispatch.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <span>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

template<typename Engine>
// NOLINTNEXTLINE(readability-function-size)
void install_routing_sinks(Engine &e)
{
    using policy_type  = typename Engine::policy_type;
    using channel_type = typename Engine::channel_type;
    using session_type = typename Engine::session_type;

    e.m_transport.on_dialed([&e](std::unique_ptr<channel_type> ch, const endpoint &ep) { e.on_dialed(std::move(ch), ep); });
    e.m_transport.on_accepted([&e](std::unique_ptr<channel_type> ch) { e.on_accepted(std::move(ch)); });
    e.m_transport.on_dial_failed([&e](const endpoint &ep, io_error) { e.m_registry.notify_dial_failed(ep); });
    e.m_build.on_lifecycle = [&e](const lifecycle_event &ev) { e.dispatch_lifecycle(ev); };
    e.m_messages.on_drop(make_drop_sink(e));
    // The data-path observation sinks post a snapshot on the borrowed executor, never inline from
    // the per-publish loop (the DoS-amplifier guard).
    e.m_messages.on_published(make_publish_sink(e));
    e.m_messages.on_delivered(make_deliver_sink(e));
    e.m_messages.on_qos_change(make_qos_change_sink(e));
    e.m_messages.on_demand_transition([&e](std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
                                      { e.m_coordinator.on_edge(node_name, fqn, dir, role); });
    e.m_messages.on_companion_route([&e](std::string_view node_name, std::string_view fqn, std::size_t bytes) -> channel_type *
                                    { return e.m_coordinator.companion_for(node_name, fqn, bytes); });
    e.m_messages.set_capture_wants_payload([&e](std::uint64_t hash) { return e.m_capture.wants_payload(hash, wire::now_timestamp_ns()); });
    // The table copies the borrowed names in and reject-and-counts on overflow, so a peer flooding
    // topics costs a counter, never an eviction and never an abort.
    e.m_messages.on_topic_edge([&e](const graph::topic_edge &edge) { e.m_topics.upsert(edge); });
    e.m_procedures.on_rpc_call(make_rpc_sink(e, &observer::on_rpc_call));
    e.m_procedures.on_rpc_serve(make_rpc_sink(e, &observer::on_rpc_serve));
    e.m_procedures.on_rpc_reply(make_rpc_sink(e, &observer::on_rpc_reply));
    e.m_messages.set_on_data_stamp([&e](const node_id &ep, std::uint64_t topic_hash) { e.m_monitor.stamp_data(ep, topic_hash); });
    // A received heartbeat asserts session presence AND refreshes the peer's discovery TTL, so an
    // actively-heartbeating peer never ages out of awareness even if its multicast announces lapse;
    // refresh is a no-op for a peer that is not currently known, inventing no awareness.
    e.m_build.on_stamp_seen = [&e](const node_id &ep)
    {
        e.m_monitor.stamp_seen(ep);
        e.m_known_peers.refresh(ep, e.now_for_aging());
        e.m_peer_liveliness.note_heartbeat(ep, e.now_for_aging());
    };
    e.m_monitor.on_liveness([&e](const liveness_event &ev) { policy_type::post(e.m_executor, [&e, ev] { e.fan_liveness(ev); }); });
    e.m_peer_liveliness.on_verdict([&e](const peer_liveliness_event &ev) { detail::post_liveliness(e, ev); });
    // The single per-tick action (NOT a second timer): emit a heartbeat to every connected peer when
    // the configured interval has elapsed (the default equals the tick cadence, so emission is
    // per-tick and wire-identical), sweep the awareness table for peers past their discovery TTL, then
    // settle the fused verdicts so a TTL expiry lands in the same tick's evaluation.
    e.m_monitor.on_tick_action(
            [&e, last_emit_ns = std::uint64_t{0}]() mutable
            {
                const std::uint64_t now      = e.now_for_aging();
                const std::uint64_t interval = static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(e.m_liveliness.heartbeat_interval).count());
                if(now - last_emit_ns >= interval)
                {
                    e.m_registry.for_each_connected([](const node_id &, session_type &s) { s.emit_heartbeat(); });
                    last_emit_ns = now;
                }
                e.sweep_aged_awareness();
                e.m_peer_liveliness.evaluate(now);
            });
    e.m_monitor.start();
}

}

#endif

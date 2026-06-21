#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINK_INSTALL_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_ROUTING_SINK_INSTALL_H

#include "plexus/io/observer.h"
#include "plexus/io/locality.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/detail/routing_sinks.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include <span>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

// Wire every node-shared sink the engine installs once at construction: the transport's
// dial/accept edges, the forwarder's drop/data-path/demand/companion sinks, the procedure
// forwarder's rpc sinks, and the liveliness monitor's liveness/tick actions. RELOCATION of the
// ctor body — the engine still OWNS every object; this only carries the wiring out of the
// over-limit ctor. Templated on the engine so it sees the concrete Policy/Transport/Clock and
// the private member-sink makers it is a friend of.
// Function over the 25-line ceiling (0 cognitive): one flat sequential wiring of every
// node-shared sink over the shared engine members; splitting the run into one-shot helpers
// scatters that wiring with no readability gain (the artificial-purity layer conventions forbid).
// Registered in EXCEPTIONS.md (function exceptions).
template<typename Engine>
// NOLINTNEXTLINE(readability-function-size)
void install_routing_sinks(Engine &e)
{
    using policy_type  = typename Engine::policy_type;
    using channel_type = typename Engine::channel_type;
    using session_type = typename Engine::session_type;

    e.m_transport.on_dialed([&e](std::unique_ptr<channel_type> ch, const endpoint &ep)
                            { e.on_dialed(std::move(ch), ep); });
    e.m_transport.on_accepted([&e](std::unique_ptr<channel_type> ch)
                              { e.on_accepted(std::move(ch)); });
    e.m_transport.on_dial_failed([&e](const endpoint &ep, io_error)
                                 { e.m_registry.notify_dial_failed(ep); });
    e.m_build.on_lifecycle = [&e](const lifecycle_event &ev) { e.dispatch_lifecycle(ev); };
    e.m_messages.on_drop(make_drop_sink(e));
    // The data-path observation sinks post a snapshot to the observer fan-out on the borrowed
    // executor, never inline from the per-publish loop (the DoS-amplifier guard); the posted
    // closure addrefs the framed owner so the buffer outlives the deferred turn.
    e.m_messages.on_published(make_publish_sink(e));
    e.m_messages.on_delivered(make_deliver_sink(e));
    e.m_messages.on_qos_change(make_qos_change_sink(e));
    // The forwarder's demand transition drives the coordinator (read same_host, run the upgrade
    // policy, acquire/release the additive ring through the injected gate).
    e.m_messages.on_demand_transition([&e](std::string_view node_name, std::string_view fqn,
                                           demand_transition dir, demand_role role)
                                      { e.m_coordinator.on_edge(node_name, fqn, dir, role); });
    // A fitting same-host message rides the minted companion ring (zero-copy); an over-cap /
    // off-host / un-minted message keeps the wire sub.channel (the dual-delivery fail-safe).
    e.m_messages.on_companion_route(
            [&e](std::string_view node_name, std::string_view fqn,
                 std::size_t bytes) -> channel_type *
            { return e.m_coordinator.companion_for(node_name, fqn, bytes); });
    // The typed fast path forces its lazy encode ONLY for a topic the policy selects at payload
    // fidelity and admits this tick, reusing the publish path's now_ns (no extra clock read). An
    // unset hook keeps the fast path zero-encode.
    e.m_messages.set_capture_wants_payload(
            [&e](std::uint64_t hash)
            { return e.m_capture.wants_payload(hash, wire::now_timestamp_ns()); });
    e.m_procedures.on_rpc_call(make_rpc_sink(e, &observer::on_rpc_call));
    e.m_procedures.on_rpc_serve(make_rpc_sink(e, &observer::on_rpc_serve));
    e.m_procedures.on_rpc_reply(make_rpc_sink(e, &observer::on_rpc_reply));
    // A data frame stamps deadline + presence; a heartbeat stamps presence only. Both stores.
    e.m_messages.set_on_data_stamp([&e](const node_id &ep, std::uint64_t topic_hash)
                                   { e.m_monitor.stamp_data(ep, topic_hash); });
    e.m_build.on_stamp_seen = [&e](const node_id &ep) { e.m_monitor.stamp_seen(ep); };
    e.m_monitor.on_liveness([&e](const liveness_event &ev)
                            { policy_type::post(e.m_executor, [&e, ev] { e.fan_liveness(ev); }); });
    // An additional per-tick action, NOT a second timer: emit a heartbeat to every connected peer
    // so a silent-but-alive publisher still asserts presence.
    e.m_monitor.on_tick_action(
            [&e]
            {
                e.m_registry.for_each_connected([](const node_id &, session_type &s)
                                                { s.emit_heartbeat(); });
            });
    e.m_monitor.start();
}

}

#endif

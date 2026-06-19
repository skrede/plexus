#ifndef HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H
#define HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H

#include "plexus/io/node_name.h"
#include "plexus/io/locality.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/reliability_requirement.h"
#include "plexus/io/observer.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/peer_session_registry.h"
#include "plexus/io/session_build_context.h"
#include "plexus/io/shm/medium_coordinator.h"
#include "plexus/io/detail/routing_sinks.h"
#include "plexus/io/detail/routing_dispatch.h"
#include "plexus/io/detail/routing_sink_install.h"

#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include <span>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::io {

// The single node-level object a future public facade drives. It OWNS the node-shared
// substrate by value (message_forwarder, procedure_forwarder, peer_session_registry,
// known_peers) and BORROWS the transport, executor, self identity, handshake bound and logger.
//
// The dial trigger is demand-driven: reach(id) is the convergence verb both the {subscribe,
// call} demand helpers and the eager knob funnel through — one build path, no second tail.
// note_peer NEVER dials under the default LAZY knob; the EAGER knob adds only an early reach.
// A PUBLISH never dials: a remote subscribe is what creates a topic's connection demand.
//
// LIFETIME: every observer fan-out and transport callback is POSTED on the BORROWED executor
// and captures `this`. The executor MUST be drained (or quiesced) before the engine is
// destroyed — the structural single-owner discipline. There is no per-post liveness guard (a
// guard reading a member would itself touch dead `this`); the owner sequences teardown.
//
// over-limit: one cohesive node-level engine; the demand-driven public verbs share the borrowed
// substrate members and the posted observer fan-out, so splitting the API scatters that shared
// state (the sink-install, sink-maker, and posted-dispatch clusters already extracted to detail/).
template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class routing_engine
{
public:
    using policy_type   = Policy;
    using executor_type = typename Policy::executor_type;
    using channel_type  = typename Policy::byte_channel_type;
    using registry_type = peer_session_registry<Policy, Transport, Clock>;
    using session_type  = peer_session<Policy>;

    // dial_eagerly is required-with-default: false (LAZY). A bool, NOT optional<bool> — its
    // absence is not meaningful, only its value is.
    routing_engine(Transport &transport, executor_type executor,
                   const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout,
                   const reconnect_config &redial, std::uint64_t redial_seed,
                   bool dial_eagerly = false, log::logger &logger = shared_null_logger(),
                   std::size_t global_default = io::global_default_max_message_bytes)
            : m_transport(transport)
            , m_executor(executor)
            , m_monitor(m_executor)
            , m_messages(global_default, logger)
            , m_procedures(executor, handshake_timeout, logger)
            , m_security_fanout{*this}
            , m_build{executor,
                      fsm_cfg,
                      handshake_timeout,
                      m_messages,
                      m_procedures,
                      redial,
                      redial_seed,
                      logger,
                      {},
                      {},
                      {},
                      {},
                      m_security_fanout,
                      {},
                      {}}
            , m_registry(transport, m_build)
            , m_coordinator(m_registry)
            , m_dial_eagerly(dial_eagerly)
    {
        detail::install_routing_sinks(*this);
    }

    // Operator-driven cold-path registration — no remote peer can grow the list.
    void add_observer(observer &o)
    {
        m_observers.push_back(&o);
        if(o.observes_data_path())
            m_capture.add_observer();
    }
    void remove_observer(observer &o)
    {
        const auto erased = std::erase(m_observers, &o);
        if(erased != 0 && o.observes_data_path())
            m_capture.remove_observer();
    }

    // The engine's posted drop sink, for an external component that routes drops through the
    // engine's fan-out via an erased on_drop verb. The maker lives in detail/.
    [[nodiscard]] auto drop_sink() { return detail::make_drop_sink(*this); }

    // NOT folded into the observer lifecycle surface — a timing lapse is a distinct event from
    // a connection edge. Fired POSTED on a missed-deadline / lease-expiry. Absent = dormant.
    void on_liveness(plexus::detail::move_only_function<void(const liveness_event &)> cb)
    {
        m_on_liveness = std::move(cb);
    }

    // Stored into the build context so every built session (and reconnect rebuild) delivers
    // through it. Set ONCE before listen/dial — a session built before this is wired delivers
    // nothing through it. The 3-arg shape carries the message_info; a bytes consumer drops it.
    void on_message_route(plexus::detail::move_only_function<
                          void(std::string_view, std::span<const std::byte>, const message_info &)>
                                  route)
    {
        m_build.on_message = std::move(route);
    }

    // The object-lane route, mirroring on_message_route. Set ONCE before listen/dial.
    void on_object_route(
            plexus::detail::move_only_function<void(std::string_view, const object_carrier &)>
                    route)
    {
        m_build.on_object = std::move(route);
    }

    void listen(const endpoint &ep) { m_transport.listen(ep); }

    // Record the (id, endpoint); reach it immediately under the eager knob only. The lazy
    // default inserts and waits for demand.
    void note_peer(const node_id &id, const endpoint &ep)
    {
        m_known.note_peer(id, ep);
        if(m_dial_eagerly)
            reach(id);
    }

    // The convergence verb both demand and eager funnel through: a connected peer is a no-op;
    // otherwise look the endpoint up, ensure the slot exists (so on_dialed builds it), and dial.
    void reach(const node_id &id)
    {
        if(m_registry.is_connected(id))
            return;
        auto ep = m_known.lookup(id);
        if(!ep)
            return;
        m_registry.ensure_slot(id, *ep, node_name_of(id));
        m_registry.driver_for(id).start();
    }

    // Reach the peer, then attach the topic through the COUNTED session path once connected
    // (routing through session->subscribe keeps the forwarder readiness-agnostic).
    //
    // reach_mask and require are two INDEPENDENT pre-dial demand gates (both required-with-
    // default any = permissive). The mask confines the subscription to the peer's delivery tier
    // (the SUBSCRIPTION's mask, distinct from the fan-out gate's PUBLISHED-topic mask); require
    // refuses a best_effort/unknown peer when reliable is demanded. Either refusal establishes
    // NO path. Both read only the endpoint scheme via known_peers (no transport_backend change);
    // a confined/strict demand toward an UNKNOWN peer is refused fail-closed (scheme_is_reliable
    // mirrors the asio selector's reliability_of_scheme).
    void subscribe(const node_id &id, std::string_view fqn, locality reach_mask = locality::any,
                   reliability_requirement require = reliability_requirement::any)
    {
        subscribe(id, fqn, subscriber_qos{}, reach_mask, require);
    }

    // The subscriber threads its OWN requested deadline/lease through the demand chain so a
    // deferred dial resurrects the real periods (remember_demand carries the qos; the register
    // seam lands them where the local monitor reads them). Periods are requested-with-default
    // (subscriber_qos{} = no deadline/liveliness monitoring); the reach/reliability gates are
    // orthogonal.
    void subscribe(const node_id &id, std::string_view fqn, const subscriber_qos &qos,
                   locality                     reach_mask = locality::any,
                   reliability_requirement      require    = reliability_requirement::any,
                   std::optional<std::uint64_t> type_id    = std::nullopt)
    {
        if(!demand_in_scope(id, reach_mask))
            return; // out-of-mask demand: establish NO path toward this peer
        if(!reliability_in_scope(id, require))
            return; // reliability-mismatched demand: establish NO path toward this peer
        reach(id);
        // Record the durable demand FIRST — before any session may exist (the async dial
        // completes later, then resurrects it through the counted path), so a lazy subscribe
        // that triggered the dial is never lost (the first-publish-loss guard).
        m_messages.remember_demand(node_name_of(id), fqn, qos, type_id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->subscribe(fqn, qos, type_id);
    }

    // PRECONDITION: unlike subscribe, a call has NO durable-demand mirror — a call issued
    // before the session completes is dropped (no queue, no error callback). The caller must
    // issue call on an already-connected peer.
    void call(const node_id &id, std::string_view fqn, std::span<const std::byte> param,
              typename procedure_forwarder<Policy>::on_response_fn on_response)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_procedures.call(session->rpc_peer(), fqn, param, std::move(on_response), std::nullopt,
                              session->session_id());
    }

    // Forget the durable demand and, when a live session carries it, detach through the counted
    // session path so the forwarder emits the wire unsubscribe on its 1->0 transition. A demand
    // never established forgets the remembered record only.
    void unsubscribe(const node_id &id, std::string_view fqn)
    {
        m_messages.forget_remembered_demand(node_name_of(id), fqn);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->unsubscribe(fqn);
    }

    // PUBLISH does NOT dial: it fans to whoever is already subscribed (the demand is the
    // remote subscribe).
    void publish(std::string_view fqn, std::span<const std::byte> payload)
    {
        m_messages.publish(fqn, payload);
    }

    bool          is_connected(const node_id &id) const { return m_registry.is_connected(id); }
    bool          is_dead(const node_id &id) const { return m_registry.is_dead(id); }
    bool          has_session(const node_id &id) const { return m_registry.has_session(id); }
    std::uint32_t attempt_count(const node_id &id) const { return m_registry.attempt_count(id); }
    session_type *session_for(const node_id &id) { return m_registry.session_for(id); }

    const known_peers    &known() const noexcept { return m_known; }
    shm::host_fingerprint local_fingerprint() const noexcept
    {
        return m_build.fsm_cfg.local_fingerprint;
    }
    message_forwarder<Policy>   &messages() noexcept { return m_messages; }
    procedure_forwarder<Policy> &procedures() noexcept { return m_procedures; }
    registry_type               &registry() noexcept { return m_registry; }
    shm::medium_coordinator<registry_type, channel_type> &coordinator() noexcept
    {
        return m_coordinator;
    }

    // Passes the consumer-sovereign upgrade-policy hook to the coordinator (default-when-unset
    // = attempt_shm_upgrade).
    void
    on_upgrade_policy(plexus::detail::move_only_function<bool(bool, shm::dispatch_hint)> policy)
    {
        m_coordinator.on_policy(std::move(policy));
    }

    // Installs the send-companion MINT gate into the coordinator — only for an shm-bearing
    // composition. The mint returns the live companion channel + its per-message route inputs.
    void on_upgrade_gate(
            plexus::detail::move_only_function<shm::companion_mint<channel_type>(std::string_view)>
                    mint)
    {
        m_coordinator.on_gate(std::move(mint));
    }

    // Installs the RECEIVE-companion gate into the coordinator — only for an shm-bearing
    // composition. The gate attaches the co-host ring as a consumer and routes drained framed
    // messages through inject_companion_receive to the matching peer session's receive path.
    void on_upgrade_receive_gate(plexus::detail::move_only_function<
                                 shm::companion_receive(std::string_view, std::string_view)>
                                         mint)
    {
        m_coordinator.on_receive_gate(std::move(mint));
    }

    // Route a drained companion-ring frame into node_name's receive path — the SAME entry the
    // wire feeds. Called POSTED (the notifier->executor bridge posts the drain), never inline
    // from a wake. A vanished peer drops the frame. A fitting message reaches the user callback
    // over SHM exactly once — the send side put it on this lane alone, never the wire.
    void inject_companion_receive(std::string_view node_name, std::span<const std::byte> frame)
    {
        if(session_type *s = m_registry.session_for_name(node_name))
            s->inject_receive(frame);
    }
    capture_policy &capture() noexcept { return m_capture; }

    // POST a node-declaration lifecycle edge over the observer snapshot, never inline from the
    // node ctor/seams. The snapshot is taken at DRAIN time (fan_out), so an observer registered
    // after the node ctor still sees the created/declared edges posted before its registration.
    void post_participant(const participant_event &ev)
    {
        Policy::post(m_executor, [this, ev]
                     { detail::fan_out(*this, [&](observer &o) { o.on_participant(ev); }); });
    }

    void post_endpoint(std::string_view fqn, const endpoint_event &ev)
    {
        Policy::post(m_executor, [this, fqn = std::string{fqn}, ev]
                     { detail::fan_out(*this, [&](observer &o) { o.on_endpoint(fqn, ev); }); });
    }

    // The destroy edge from ~node. UNLIKE the in-life edges, the engine is destroyed right
    // after ~node returns, BEFORE the executor drains, so the closure must NOT dereference the
    // engine: it captures an externally-owned observer SNAPSHOT by value and fans with no engine
    // touch (the owner pumps the executor after the node returns, per the teardown contract).
    void post_participant_teardown(const participant_event &ev)
    {
        Policy::post(m_executor,
                     [snapshot = m_observers, ev]
                     {
                         for(auto *o : snapshot)
                             o->on_participant(ev);
                     });
    }

private:
    template<typename E>
    friend void detail::install_routing_sinks(E &);
    template<typename E>
    friend auto detail::make_drop_sink(E &);
    template<typename E>
    friend auto detail::make_wire_sink(E &, const node_id &);
    template<typename E>
    friend auto detail::make_publish_sink(E &);
    template<typename E>
    friend auto detail::make_deliver_sink(E &);
    template<typename E>
    friend auto detail::make_qos_change_sink(E &);
    template<typename E, typename Edge>
    friend auto detail::make_rpc_sink(E &, Edge);
    template<typename E, typename Deliver>
    friend void detail::fan_out(E &, Deliver);
    template<typename E>
    friend void detail::post_edge(E &, const lifecycle_event &,
                                  void (observer::*)(const node_id &, std::string_view, peer_kind));
    template<typename E>
    friend void detail::post_rejected(E &, const lifecycle_event &);
    template<typename E>
    friend void detail::post_drop(E &, const detail::drop_event &);
    template<typename E>
    friend void detail::post_security(E &, const security_event &);
    template<typename E>
    friend void detail::post_wire(E &, recording::wire_direction, std::uint64_t, const node_id &,
                                  std::span<const std::byte>);

    // Forwards each session's security edge into the engine's posted fan-out. Installed by
    // reference — the engine outlives every session built from the context.
    struct security_fanout : observer
    {
        routing_engine &engine;
        explicit security_fanout(routing_engine &e)
                : engine(e)
        {
        }
        void on_security(const security_event &ev) override { detail::post_security(engine, ev); }
    };

    // Does the reach mask admit the peer's delivery tier? The default any admits every peer; a
    // confined mask classifies the tier from the endpoint scheme and refuses an out-of-scope or
    // UNKNOWN peer (fail-closed — never establish a path we cannot prove is in scope).
    bool demand_in_scope(const node_id &id, locality reach_mask) const
    {
        if(reach_mask == locality::any)
            return true;
        auto ep = m_known.lookup(id);
        if(!ep)
            return false;
        return any_set(reach_mask, tier_of(ep->scheme));
    }

    // Does the required reliability admit the peer's transport class? The default any admits
    // every peer; a strict reliable classifies from the endpoint scheme (scheme_is_reliable
    // mirrors the asio selector's reliability_of_scheme) and refuses a best_effort or UNKNOWN
    // peer (fail-closed — never admit a strict demand over a transport we cannot prove reliable).
    bool reliability_in_scope(const node_id &id, reliability_requirement require) const
    {
        if(require == reliability_requirement::any)
            return true;
        auto ep = m_known.lookup(id);
        if(!ep)
            return false;
        return scheme_is_reliable(ep->scheme);
    }

    // CORRELATION by endpoint, NOT by arrival order: a real async transport completes
    // concurrent dials OUT OF ORDER, so only the dialed endpoint deterministically identifies
    // the originating slot.
    void on_dialed(std::unique_ptr<channel_type> channel, const endpoint &dialed)
    {
        wire_channel_drop(*channel);
        // The dialed endpoint resolves to the peer node_id (the wire-capture join key); an
        // unresolved endpoint binds the default id (capture still records, attribution then
        // comes from the synthetic identity build_session supplies).
        wire_channel_capture(*channel, m_registry.id_for_endpoint(dialed).value_or(node_id{}));
        m_registry.build_session_for_endpoint(dialed, std::move(channel));
    }

    void on_accepted(std::unique_ptr<channel_type> channel)
    {
        wire_channel_drop(*channel);
        // The peer's real id arrives only in the handshake, so the capture install is threaded
        // through accept_session, which binds the synthetic inbound identity before the build.
        m_registry.accept_session(std::move(channel), [this](channel_type &ch, const node_id &peer)
                                  { wire_channel_capture(ch, peer); });
    }

    // Install the posted drop_sink onto a channel that carries the optional on_drop edge. A
    // channel whose tier surfaces none is left untouched at compile time — the sink is routing
    // policy the engine owns, not a per-backend obligation. The drop stays POSTED.
    void wire_channel_drop(channel_type &channel)
    {
        if constexpr(requires { channel.on_drop(detail::make_drop_sink(*this)); })
            channel.on_drop(detail::make_drop_sink(*this));
    }

    // Install the posted wire-capture sink onto a channel that carries the on_wire edge. A bare
    // channel that never composed the recording_channel decorator has none, so the compiler
    // elides this: the wire tier is STRUCTURALLY absent, not a runtime branch. peer is the slot
    // identity — the wire_record's cross-node join key.
    void wire_channel_capture(channel_type &channel, const node_id &peer)
    {
        if constexpr(requires { channel.on_wire(detail::make_wire_sink(*this, peer)); })
            channel.on_wire(detail::make_wire_sink(*this, peer));
    }

    // Fans over a SNAPSHOT of m_observers so a callback that (un)registers an observer mutates
    // the live list without invalidating the in-flight iteration: a same-turn remove is honored,
    // a same-turn add takes effect on the next posted edge.
    void dispatch_lifecycle(const lifecycle_event &ev)
    {
        // Register at READY, NOT connected: the subscribe loop has drained by ready, so the
        // remembered demand (carrying the subscriber's OWN periods) is populated and the monitor
        // arms the real periods. Deregister before the executor quiesces so a firing tick never
        // touches a dead endpoint.
        if(ev.edge == lifecycle_edge::ready)
            register_endpoint(ev.id, ev.node_name);
        else if(ev.edge == lifecycle_edge::disconnected || ev.edge == lifecycle_edge::dead)
        {
            m_monitor.deregister_endpoint(ev.id);
            // A torn-down peer can no longer map the ring, so the additive lane is dropped (a
            // surviving reconnect re-drives the demand edge).
            m_coordinator.on_peer_dead(ev.node_name);
        }

        switch(ev.edge)
        {
            case lifecycle_edge::connected:
                return detail::post_edge(*this, ev, &observer::on_peer_connected);
            case lifecycle_edge::disconnected:
                return detail::post_edge(*this, ev, &observer::on_peer_disconnected);
            case lifecycle_edge::reconnected:
                return detail::post_edge(*this, ev, &observer::on_peer_reconnected);
            case lifecycle_edge::dead: return detail::post_edge(*this, ev, &observer::on_peer_dead);
            case lifecycle_edge::ready:
                return detail::post_edge(*this, ev, &observer::on_peer_ready);
            case lifecycle_edge::rejected: return detail::post_rejected(*this, ev);
        }
    }

    // Arm the monitor per (endpoint, topic) the subscriber demanded. The remembered demand
    // carries the subscriber's OWN periods; a 0 period leaves that axis inert.
    void register_endpoint(const node_id &id, const std::string &node_name)
    {
        for(const auto &demand : m_messages.remembered_topics(node_name))
            m_monitor.register_endpoint(id, wire::fqn_topic_hash(demand.fqn),
                                        demand.qos.requested_deadline_ns,
                                        demand.qos.requested_lease_ns);
    }

    void fan_liveness(const liveness_event &ev)
    {
        if(m_on_liveness)
            m_on_liveness(ev);
    }

    Transport                                           &m_transport;
    executor_type                                        m_executor;
    liveliness_monitor<Policy, Clock>                    m_monitor;
    message_forwarder<Policy>                            m_messages;
    procedure_forwarder<Policy>                          m_procedures;
    security_fanout                                      m_security_fanout;
    session_build_context<Policy>                        m_build;
    registry_type                                        m_registry;
    shm::medium_coordinator<registry_type, channel_type> m_coordinator;
    known_peers                                          m_known;
    std::vector<observer *>                              m_observers;
    // The single capture-decision point: owns the per-topic selection rules AND the data-path
    // observer-presence count, so the hot sink heads consult one gate, not a separate boolean.
    capture_policy                                                   m_capture;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness;
    bool                                                             m_dial_eagerly;
};

}

#endif

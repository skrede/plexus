#ifndef HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H
#define HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H

#include "plexus/io/node_name.h"
#include "plexus/io/locality.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/reliability_requirement.h"
#include "plexus/io/observer.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/peer_session_registry.h"
#include "plexus/io/session_build_context.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include <chrono>
#include <memory>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::io {

// The single node-level object a future public facade drives (exercised here
// through the oracle harness). It OWNS the node-shared substrate by value — one
// message_forwarder + one procedure_forwarder (ownership lifted UP from the
// per-connection harness; the forwarders are unchanged), the peer_session_registry
// (the node_id -> slot map), and the known_peers awareness table — and BORROWS the
// transport, executor, self identity, handshake bound and logger by reference.
//
// The dial trigger is demand-driven: reach(id) is the convergence verb both the
// {subscribe, call} demand helpers and the eager knob funnel through. note_peer
// records awareness and NEVER dials under the default LAZY knob; the opt-in EAGER
// knob adds only an early reach off note_peer — it introduces NO second build
// path, because both knobs share the registry's single on_dialed -> build-from-
// record -> start() tail. A PUBLISH never dials: a remote subscribe is what
// creates a topic's connection demand, so publish only fans to who is already
// connected. transport.on_dialed / on_accepted / on_dial_failed are wired ONCE in
// the constructor.
//
// LIFETIME: every observer fan-out and transport callback is POSTED on the BORROWED
// executor and captures `this`; the engine does not own the executor. The executor
// MUST be drained (or quiesced) before the engine is destroyed — the same structural
// single-owner discipline every posted callback in plexus relies on. There is no
// per-post liveness guard (a guard reading a member would itself touch dead `this`,
// and a shared alive-token contradicts the no-shared-ownership model); the owner
// sequences teardown.
template <typename Policy, typename Transport, typename Clock = std::chrono::steady_clock>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class routing_engine
{
public:
    using executor_type = typename Policy::executor_type;
    using channel_type = typename Policy::byte_channel_type;
    using registry_type = peer_session_registry<Policy, Transport, Clock>;
    using session_type = peer_session<Policy>;

    // dial_eagerly is required-with-default: false (LAZY) is the default the caller
    // may override to true (EAGER). It is a bool, NOT a std::optional<bool> — its
    // absence is not meaningful, only its value is.
    routing_engine(Transport &transport, executor_type executor,
                   const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout,
                   const reconnect_config &redial, std::uint64_t redial_seed,
                   bool dial_eagerly = false, log::logger &logger = shared_null_logger())
        : m_transport(transport)
        , m_executor(executor)
        , m_monitor(m_executor)
        , m_messages(m_executor)
        , m_procedures(executor, handshake_timeout, logger)
        , m_security_fanout{*this}
        , m_build{executor, fsm_cfg, handshake_timeout, m_messages, m_procedures,
                  redial, redial_seed, logger, {}, {}, {}, {}, m_security_fanout, {}, {}}
        , m_registry(transport, m_build)
        , m_dial_eagerly(dial_eagerly)
    {
        m_transport.on_dialed([this](std::unique_ptr<channel_type> ch, const endpoint &ep) { on_dialed(std::move(ch), ep); });
        m_transport.on_accepted([this](std::unique_ptr<channel_type> ch) { on_accepted(std::move(ch)); });
        m_transport.on_dial_failed([this](const endpoint &ep, io_error) { m_registry.notify_dial_failed(ep); });
        m_build.on_lifecycle = [this](const lifecycle_event &ev) { dispatch_lifecycle(ev); };
        // The egress shed routes through the posted drop_sink (the receive-side causes
        // already install it at their sites) so an egress overflow reaches the observer
        // POSTED, never inline from the per-publish fan loop.
        m_messages.on_drop(drop_sink());
        // The receive path stamps the one monitor: a data frame stamps deadline +
        // presence (set_on_data_stamp), a heartbeat stamps presence only (on_stamp_seen
        // routed through the build context to each session). Both are plain stores.
        m_messages.set_on_data_stamp(
            [this](const node_id &ep, std::uint64_t topic_hash) { m_monitor.stamp_data(ep, topic_hash); });
        m_build.on_stamp_seen = [this](const node_id &ep) { m_monitor.stamp_seen(ep); };
        // Route a fired timing event up the engine's own posted observer seam (mirrors
        // dispatch_lifecycle's posted fan-out — the carrier copies by value into the turn).
        m_monitor.on_liveness([this](const liveness_event &ev) {
            Policy::post(m_executor, [this, ev] { fan_liveness(ev); });
        });
        // The keepalive heartbeat emit rides the SAME single tick (an additional per-tick
        // action, NOT a second timer): on each tick, emit a heartbeat to every connected
        // peer so a silent-but-alive publisher still asserts presence.
        m_monitor.on_tick_action([this] {
            m_registry.for_each_connected([](const node_id &, session_type &s) { s.emit_heartbeat(); });
        });
        m_monitor.start();
    }

    // Register/unregister a session observer (lifecycle, drop, and security edges). The
    // list is the registry, not a wire-exposed param: the add/remove API takes a const&
    // and stores the address. Operator-driven cold-path registration — no remote peer
    // can grow it.
    void add_observer(observer &o) { m_observers.push_back(&o); }
    void remove_observer(observer &o) { std::erase(m_observers, &o); }

    // The seam a drop site posts a coalesced event into. It is a value-capturing,
    // engine-posted callable: every drop fans out on the BORROWED executor over a
    // snapshot, NEVER synchronously from the site — the per-packet-inline-fire DoS guard
    // (a receive-side site holds this and increments its own occupancy counter, then
    // hands the event here). The returned sink owns no lifetime; the engine outlives the
    // channels it installs the sink into (the single-owner teardown discipline).
    [[nodiscard]] plexus::detail::move_only_function<void(const detail::drop_event &)> drop_sink()
    {
        return [this](const detail::drop_event &ev) { post_drop(ev); };
    }

    // The subscriber-side timing-gate observable (FORK-B): a dedicated settable
    // callback the engine fires (POSTED on the executor) when the one monitor reports a
    // missed-deadline or a lease-expiry. It is NOT folded into the observer
    // lifecycle surface — a timing lapse is a distinct event from a connection edge.
    // Absent = dormant. Cold-path registration.
    void on_liveness(plexus::detail::move_only_function<void(const liveness_event &)> cb)
    {
        m_on_liveness = std::move(cb);
    }

    // The node-shared receive route: stored into the build context so EVERY
    // subsequently built session (and every reconnect rebuild) delivers data through
    // it. Set ONCE before listen/dial — a session built before the route is wired
    // delivers nothing through it. The 3-arg shape carries the message_info; a
    // bytes-only consumer drops it.
    void on_message_route(plexus::detail::move_only_function<
                          void(std::string_view, std::span<const std::byte>,
                               const message_info &)> route)
    {
        m_build.on_message = std::move(route);
    }

    // The node-shared object-lane route, mirroring on_message_route exactly: stored
    // into the build context so EVERY subsequently built session (and every reconnect
    // rebuild) delivers a process-tier object handle through it. Set ONCE before
    // listen/dial.
    void on_object_route(plexus::detail::move_only_function<
                         void(std::string_view, const object_carrier &)> route)
    {
        m_build.on_object = std::move(route);
    }

    void listen(const endpoint &ep) { m_transport.listen(ep); }

    // Awareness: record the (id, endpoint) and, under the eager knob ONLY, reach it
    // immediately. The lazy default inserts and waits for demand.
    void note_peer(const node_id &id, const endpoint &ep)
    {
        m_known.note_peer(id, ep);
        if(m_dial_eagerly)
            reach(id);
    }

    // The convergence verb: a connected peer is a no-op; otherwise look the endpoint
    // up, ensure the slot exists, record this dial's target so on_dialed builds the
    // right slot, and dial. Both demand and eager funnel here.
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

    // A demand verb: reach the named peer, then attach the topic through the COUNTED
    // session path once connected (the attach is what makes a publish flow). Routing
    // through session->subscribe lets the session observe its own wire emit and own
    // the readiness count — the forwarder stays readiness-agnostic.
    //
    // reach_mask is the SUBSCRIPTION's own locality confinement (a category-2 required-
    // with-default: any = no confinement, existing callers unchanged). It is the demand-
    // side half of the airtight guarantee: if the mask excludes the target peer's tier
    // (classified from the endpoint scheme the engine already holds), the demand is
    // REFUSED outright — no demand remembered, no reach(id)/dial, no subscribe over that
    // out-of-scope transport. This needs NO transport_backend change; it reads the
    // endpoint scheme via known_peers. (The fan-out gate reads the PUBLISHED topic's
    // mask; this reads the SUBSCRIPTION's own mask — two independent gates.) A confined
    // topic's hard delivery guarantee is enforced PRODUCER-side at the fan-out gate; this
    // demand gate guards the subscriber's own scope, not the topic's confinement.
    //
    // require is the SUBSCRIPTION's own reliability requirement — a SECOND, INDEPENDENT
    // required-with-default gate of the same shape (any = permissive, anything connects,
    // the default; reliable = strict, refuse a demand whose peer transport is best_effort
    // or unknown). Both gates run pre-dial; either refusal establishes NO path. This too
    // reads only the endpoint scheme the engine already holds (NO transport_backend
    // change). The scheme->reliability mapping is consistent with the asio selector's
    // (scheme_is_reliable mirrors reliability_of_scheme).
    void subscribe(const node_id &id, std::string_view fqn, locality reach_mask = locality::any,
                   reliability_requirement require = reliability_requirement::any)
    {
        subscribe(id, fqn, subscriber_qos{}, reach_mask, require);
    }

    // The qos-carrying subscribe: the subscriber threads its OWN requested deadline /
    // lease through the demand chain so the SUBSCRIBING node stores them locally
    // (remember_demand carries the qos so a deferred dial resurrects the real periods;
    // session->subscribe -> attach -> add_subscriber land them in the registry the
    // local monitor reads via qos_for_subscriber at the register seam). The periods are
    // requested-with-default: the 4-param overload delegates here with subscriber_qos{}
    // (absence = no deadline/liveliness monitoring). The reach/reliability gates are
    // orthogonal to the qos and unchanged.
    void subscribe(const node_id &id, std::string_view fqn, const subscriber_qos &qos,
                   locality reach_mask = locality::any,
                   reliability_requirement require = reliability_requirement::any,
                   std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(!demand_in_scope(id, reach_mask))
            return;   // out-of-mask demand: establish NO path toward this peer
        if(!reliability_in_scope(id, require))
            return;   // reliability-mismatched demand: establish NO path toward this peer
        reach(id);
        // Record the durable demand FIRST — before any session may exist (an async
        // dial completes later). The session resurrects the recorded demand through the
        // counted path on completion, so a lazy subscribe that triggered the dial is
        // never lost (the first-publish-loss guard). If the session is already complete,
        // attach now so the demand takes effect immediately on the live connection.
        m_messages.remember_demand(node_name_of(id), fqn, qos, type_id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->subscribe(fqn, qos, type_id);
    }

    // A demand verb: reach then call through the owned procedure_forwarder.
    // PRECONDITION: unlike subscribe (whose demand is remembered and resurrected on
    // completion), a call has NO durable-demand mirror — a call issued before the
    // session completes is dropped (no queue, no error callback). The caller must
    // issue call on an already-connected peer; a deferred-call-demand queue is a
    // tracked future concern, not silently free here.
    void call(const node_id &id, std::string_view fqn, std::span<const std::byte> param,
              typename procedure_forwarder<Policy>::on_response_fn on_response)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_procedures.call(session->rpc_peer(), fqn, param, std::move(on_response),
                              std::nullopt, session->session_id());
    }

    // The peer-targeted retire of a topic demand, mirroring subscribe's shape: forget
    // the durable demand and, when a live session carries it, detach through the
    // counted session path so the forwarder's per-(peer, fqn) gate emits the wire
    // unsubscribe on its 1->0 transition. A demand never established (no session, or a
    // session that never completed the attach) forgets the remembered record only.
    void unsubscribe(const node_id &id, std::string_view fqn)
    {
        m_messages.forget_remembered_demand(node_name_of(id), fqn);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->unsubscribe(fqn);
    }

    // PUBLISH does NOT dial: it fans through the message_forwarder to whoever is
    // already subscribed and triggers NO reach (the demand is the remote subscribe).
    void publish(std::string_view fqn, std::span<const std::byte> payload)
    {
        m_messages.publish(fqn, payload);
    }

    bool is_connected(const node_id &id) const { return m_registry.is_connected(id); }
    bool is_dead(const node_id &id) const { return m_registry.is_dead(id); }
    bool has_session(const node_id &id) const { return m_registry.has_session(id); }
    std::uint32_t attempt_count(const node_id &id) const { return m_registry.attempt_count(id); }
    session_type *session_for(const node_id &id) { return m_registry.session_for(id); }

    const known_peers &known() const noexcept { return m_known; }
    message_forwarder<Policy> &messages() noexcept { return m_messages; }
    procedure_forwarder<Policy> &procedures() noexcept { return m_procedures; }
    registry_type &registry() noexcept { return m_registry; }

private:
    // The build-context observer the registry routes each session's security edge into:
    // it forwards into the engine's posted fan-out, so the install is by reference (the
    // engine outlives every session built from the context) and every emit stays posted.
    struct security_fanout : observer
    {
        routing_engine &engine;
        explicit security_fanout(routing_engine &e) : engine(e) {}
        void on_security(const security_event &ev) override { engine.post_security(ev); }
    };

    // The demand-side confinement gate: does a subscription's reach mask admit the
    // target peer's delivery tier? The default any admits every peer (existing callers
    // are never refused). A confined mask classifies the tier from the endpoint scheme
    // the engine already holds (known_peers) and refuses an out-of-scope peer; a confined
    // demand toward an UNKNOWN peer is also refused (fail-closed — never establish a path
    // we cannot prove is in scope).
    bool demand_in_scope(const node_id &id, locality reach_mask) const
    {
        if(reach_mask == locality::any)
            return true;
        auto ep = m_known.lookup(id);
        if(!ep)
            return false;
        return any_set(reach_mask, tier_of(ep->scheme));
    }

    // The demand-side reliability gate: does a subscription's required reliability admit
    // the target peer's transport class? The default `any` admits every peer (the
    // permissive default — existing callers are never refused). A strict `reliable`
    // requirement classifies the peer's reliability from the endpoint scheme the engine
    // already holds (scheme_is_reliable, the engine-side mirror of the asio selector's
    // reliability_of_scheme) and refuses a best_effort ("udp") peer; a strict demand
    // toward an UNKNOWN peer is also refused (fail-closed — never admit a strict-reliable
    // demand over a transport we cannot prove is reliable).
    bool reliability_in_scope(const node_id &id, reliability_requirement require) const
    {
        if(require == reliability_requirement::any)
            return true;
        auto ep = m_known.lookup(id);
        if(!ep)
            return false;
        return scheme_is_reliable(ep->scheme);
    }

    // The single dial-success tail. CORRELATION by endpoint, NOT by arrival order:
    // the transport hands back the endpoint THIS channel dialed, so it routes to the
    // slot that dialed that endpoint. A real async transport completes concurrent
    // dials OUT OF ORDER, so the arrival sequence is not the dial sequence — only the
    // endpoint deterministically identifies the originating slot.
    void on_dialed(std::unique_ptr<channel_type> channel, const endpoint &dialed)
    {
        wire_channel_drop(*channel);
        m_registry.build_session_for_endpoint(dialed, std::move(channel));
    }

    void on_accepted(std::unique_ptr<channel_type> channel)
    {
        wire_channel_drop(*channel);
        m_registry.accept_session(std::move(channel));
    }

    // Install the posted drop_sink onto a freshly minted channel that carries the
    // optional on_drop edge — the SAME edge m_messages.on_drop(drop_sink()) gives
    // egress, threaded here at the single point every backend's dialed/accepted
    // channel reaches the engine. A channel whose tier surfaces no drop edge (on_drop
    // is not a byte_channel verb; the multiplexer's erased channel exposes none) is
    // left untouched at compile time — the sink is routing policy the engine owns, not
    // a per-backend obligation. The drop stays POSTED: the inproc unmatched-partner
    // edge fires off the bus step-loop and the shm send-ring verdict reaches the
    // observer through drop_sink(), never inline.
    void wire_channel_drop(channel_type &channel)
    {
        if constexpr(requires { channel.on_drop(drop_sink()); })
            channel.on_drop(drop_sink());
    }

    // The session→observer fan-out. Every edge is delivered POSTED on the executor,
    // never synchronously from the fire-site: the posted lambda captures the event
    // BY VALUE (its owned node_name string copies into the turn). The delivery fans
    // out over a SNAPSHOT of m_observers, so a callback that (un)registers an observer
    // mutates the live list without invalidating the in-flight iteration: a same-turn
    // remove is honored (the unregistered observer is skipped), and a same-turn add
    // takes effect only on the next posted edge. rejected fans the FSM refusal reason;
    // every other edge fans the peer_kind.
    void dispatch_lifecycle(const lifecycle_event &ev)
    {
        // Register the endpoint's liveness state at the READY edge (NOT connected): the
        // subscribe loop has drained by ready, so the subscriber's remembered demand —
        // which carries its OWN requested periods — is populated and the monitor arms
        // the real periods. Deregister at disconnected/dead BEFORE the executor
        // quiesces, so a firing tick never touches a dead endpoint.
        if(ev.edge == lifecycle_edge::ready)
            register_endpoint(ev.id, ev.node_name);
        else if(ev.edge == lifecycle_edge::disconnected || ev.edge == lifecycle_edge::dead)
            m_monitor.deregister_endpoint(ev.id);

        switch(ev.edge)
        {
            case lifecycle_edge::connected:    return post_edge(ev, &observer::on_peer_connected);
            case lifecycle_edge::disconnected: return post_edge(ev, &observer::on_peer_disconnected);
            case lifecycle_edge::reconnected:  return post_edge(ev, &observer::on_peer_reconnected);
            case lifecycle_edge::dead:         return post_edge(ev, &observer::on_peer_dead);
            case lifecycle_edge::ready:        return post_edge(ev, &observer::on_peer_ready);
            case lifecycle_edge::rejected:     return post_rejected(ev);
        }
    }

    // Arm the monitor for each topic the subscriber demanded on this endpoint. The
    // remembered demand carries the subscriber's OWN requested periods (threaded
    // through subscribe(id, fqn, qos)); a 0 period leaves that axis inert. Keyed by the
    // peer's node_id and the topic_hash so the deadline is per-(endpoint, topic).
    void register_endpoint(const node_id &id, const std::string &node_name)
    {
        for(const auto &demand : m_messages.remembered_topics(node_name))
            m_monitor.register_endpoint(id, wire::fqn_topic_hash(demand.fqn),
                                        demand.qos.requested_deadline_ns,
                                        demand.qos.requested_lease_ns);
    }

    // Fan a fired timing event to the engine's settable observable (posted turn).
    void fan_liveness(const liveness_event &ev)
    {
        if(m_on_liveness)
            m_on_liveness(ev);
    }

    // Deliver to each observer over a snapshot, skipping any unregistered mid-turn, so
    // a callback may safely (un)register observers without corrupting the fan-out.
    template<class Deliver>
    void fan_out(Deliver deliver)
    {
        const auto snapshot = m_observers;
        for(auto *o : snapshot)
            if(std::find(m_observers.begin(), m_observers.end(), o) != m_observers.end())
                deliver(*o);
    }

    void post_edge(const lifecycle_event &ev,
                   void (observer::*edge)(const node_id &, std::string_view, peer_kind))
    {
        Policy::post(m_executor, [this, ev, edge] {
            fan_out([&](observer &o) { (o.*edge)(ev.id, ev.node_name, ev.kind); });
        });
    }

    void post_rejected(const lifecycle_event &ev)
    {
        Policy::post(m_executor, [this, ev] {
            fan_out([&](observer &o) { o.on_peer_rejected(ev.id, ev.node_name, ev.reason); });
        });
    }

    // POST the drop on the borrowed executor, capturing the POD by value into the turn
    // (all scalars + a node_id — a cheap, lifetime-free copy). This is the indirection
    // that keeps a per-packet drop site off the synchronous observer path.
    void post_drop(const detail::drop_event &ev)
    {
        Policy::post(m_executor, [this, ev] {
            fan_out([&](observer &o) { o.on_drop(ev); });
        });
    }

    // POST a security transition over the same snapshot fan-out the lifecycle and drop
    // edges use, capturing the flat POD by value — never inline from the session's
    // teardown/refusal frame.
    void post_security(const security_event &ev)
    {
        Policy::post(m_executor, [this, ev] {
            fan_out([&](observer &o) { o.on_security(ev); });
        });
    }


    Transport &m_transport;
    executor_type m_executor;
    liveliness_monitor<Policy, Clock> m_monitor;
    message_forwarder<Policy> m_messages;
    procedure_forwarder<Policy> m_procedures;
    security_fanout m_security_fanout;
    session_build_context<Policy> m_build;
    registry_type m_registry;
    known_peers m_known;
    std::vector<observer *> m_observers;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness;
    bool m_dial_eagerly;
};

}

#endif

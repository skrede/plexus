#ifndef HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H
#define HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/io/locality.h"
#include "plexus/io/observer.h"
#include "plexus/io/node_name.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/peer_liveliness.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/route_options.h"
#include "plexus/io/report_options.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/upgrade_coordinator.h"
#include "plexus/io/peer_session_registry.h"
#include "plexus/io/peer_report_emitter.h"
#include "plexus/io/session_build_context.h"
#include "plexus/io/reliability_requirement.h"

#include "plexus/graph/graph_change.h"
#include "plexus/graph/topic_type_table.h"
#include "plexus/graph/vector_graph_change_log.h"

#include "plexus/io/detail/routing_sinks.h"
#include "plexus/io/detail/routing_dispatch.h"
#include "plexus/io/detail/routing_sink_install.h"
#include "plexus/io/detail/peer_report_consumers.h"

#include "plexus/graph/topic_record.h"

#include "plexus/log/logger.h"

#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <map>
#include <set>
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

// The default liveliness storage selector: the monitor and arbiter tables both take the heap-backed
// std::map backends. A constrained-target build passes a selector aliasing the fixed-capacity twins.
struct default_liveliness_storage
{
    using monitor = std_map_liveness_storage;
    using arbiter = std_map_liveliness_peer_storage;
};

template<typename Policy, typename Transport, typename Clock = std::chrono::steady_clock, typename PeerStorage = std_map_peer_storage,
         typename TopicStorage = graph::std_map_topic_storage, typename LivelinessStorage = default_liveliness_storage,
         typename GraphChangeLog = graph::vector_graph_change_log, typename PeerReportEmitter = null_peer_report_emitter>
    requires plexus::Policy<Policy> && transport_backend<Transport, Policy>
class routing_engine
{
    struct security_fanout : observer
    {
        routing_engine &engine;

        explicit security_fanout(routing_engine &e)
                : engine(e)
        {
        }

        void on_security(const security_event &ev) override
        {
            detail::post_security(engine, ev);
        }
    };

public:
    using policy_type              = Policy;
    using peer_storage_type        = PeerStorage;
    using topic_storage_type       = TopicStorage;
    using peer_report_emitter_type = PeerReportEmitter;
    using session_type       = peer_session<Policy>;
    using executor_type      = typename Policy::executor_type;
    using channel_type       = typename Policy::byte_channel_type;
    using registry_type      = peer_session_registry<Policy, Transport, Clock>;

    routing_engine(Transport &transport, executor_type executor, const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout, const reconnect_config &redial,
                   std::uint64_t redial_seed, log::logger &logger, bool dial_eagerly = false, std::size_t global_default = io::global_default_max_message_bytes,
                   io::liveliness_options live = {}, io::route_options routes = {}, io::report_options report = {})
            : m_dial_eagerly(dial_eagerly)
            , m_liveliness(live)
            , m_route_options(routes)
            , m_report(io::make_report_ctx(report))
            , m_transport(transport)
            , m_executor(executor)
            , m_security_fanout{*this}
            , m_messages(logger, global_default)
            , m_procedures(executor, handshake_timeout, logger)
            , m_build{executor, fsm_cfg, handshake_timeout, m_messages, m_procedures, redial, redial_seed, logger, {}, {}, {}, {}, m_security_fanout, {}, {}}
            , m_registry(transport, m_build)
            , m_monitor(m_executor)
            , m_peer_liveliness(m_liveliness)
            , m_coordinator(m_registry)
    {
        detail::install_routing_sinks(*this);
    }

    void add_observer(observer &o)
    {
        m_observers.emplace_back(o);
        if(o.observes_data_path())
            m_capture.add_observer();
        if(o.observes_liveliness())
            m_peer_liveliness.add_subscriber();
        if(o.observes_graph())
            ++m_graph_subscribers;
    }

    void remove_observer(observer &o)
    {
        const auto erased = std::erase_if(m_observers, [&](const std::reference_wrapper<observer> &w) { return &w.get() == &o; });
        if(erased != 0 && o.observes_data_path())
            m_capture.remove_observer();
        if(erased != 0 && o.observes_liveliness())
            m_peer_liveliness.remove_subscriber();
        if(erased != 0 && o.observes_graph())
            --m_graph_subscribers;
    }

    auto drop_sink()
    {
        return detail::make_drop_sink(*this);
    }

    void on_liveness(plexus::detail::move_only_function<void(const liveness_event &)> cb)
    {
        m_on_liveness_cb = std::move(cb);
    }

    void on_message_route(plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> route)
    {
        m_build.on_message = std::move(route);
    }

    void on_object_route(plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> route)
    {
        m_build.on_object = std::move(route);
    }

    void listen(const endpoint &ep)
    {
        m_transport.listen(ep);
    }

    // The general point-to-point verb for an endpoint discovery cannot advertise (a serial port, a
    // pre-known socket): mint a stable provisional id from the endpoint and drive the same slot path
    // reach uses. The real peer id arrives in the handshake; the slot stays under the provisional id.
    void dial(const endpoint &ep)
    {
        const auto id = endpoint_id(ep);
        if(m_registry.is_connected(id))
            return;
        if(!m_registry.ensure_slot(id, ep, node_name_of(id)))
            return;
        m_registry.driver_for(id).start();
    }

    void note_peer(const node_id &id, const endpoint &ep)
    {
        note_peer(id, ep, now_for_aging());
    }

    void note_peer(const node_id &id, const endpoint &ep, std::uint64_t now)
    {
        const bool changed = m_known_peers.note_peer(id, ep, now);
        m_peer_liveliness.note_awareness(id, now);
        bump_graph_generation(changed, id, graph::change_kind::appeared);
        if(m_dial_eagerly)
            reach(id);
    }

    // Drops a peer's awareness and denies any redial its teardown armed: a goodbye withdraws the
    // suggestion reach() gates on, so an in-flight or armed reconnect for the peer is cancelled
    // rather than left to resurrect it. An established session is untouched — it ends on its own
    // teardown, not this call.
    void forget(const node_id &id)
    {
        release_all_reporter_load(id);
        const bool changed = m_known_peers.forget(id);
        m_registry.deny_redial(id);
        m_peer_liveliness.note_awareness_lost(id);
        relay_withdraw(id);
        if(m_reported.erase(id) != 0)
            m_topics.remove_node(id);
        bump_graph_generation(changed, id, graph::change_kind::disappeared);
    }

    // The receive gate chain for an inbound peer_report about a THIRD-party origin: fail-closed
    // against the ORIGIN universe, then a self-guard, then a reporter-is-origin guard (a peer
    // self-reporting must not install a transitive twin beside its own direct row), all BEFORE any
    // awareness mutation. The hop budget and per-origin dedup run in the note path. A reported
    // candidate is via-only — it never dials and never feeds the direct-peer liveliness arbiter.
    void ingest_peer_report(const node_id &reporter, const wire::peer_report &pr)
    {
        if(!detail::report_universe_admits(m_report, pr))
            return;
        if(pr.origin == m_build.fsm_cfg.self_id || pr.origin == reporter)
            return;
        note_reported_candidate(reporter, pr);
    }

    void reach(const node_id &id)
    {
        if(m_registry.is_connected(id))
            return;
        auto ep = m_known_peers.lookup(id);
        if(!ep)
            return;
        if(!m_registry.ensure_slot(id, *ep, node_name_of(id)))
            return;
        m_registry.driver_for(id).start();
    }

    void subscribe(const node_id &id, std::string_view fqn, locality reach_mask = locality::any, reliability_requirement require = reliability_requirement::any)
    {
        subscribe(id, fqn, subscriber_qos{}, reach_mask, require);
    }

    void subscribe(const node_id &id, std::string_view fqn, const subscriber_qos &qos, locality reach_mask = locality::any,
                   reliability_requirement require = reliability_requirement::any, std::optional<std::uint64_t> type_id = std::nullopt, std::string_view type_name = {})
    {
        if(!demand_in_scope(id, reach_mask))
            return;
        if(!reliability_in_scope(id, require))
            return;
        reach(id);

        m_messages.remember_demand(node_name_of(id), fqn, qos, type_id, type_name);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->subscribe(fqn, qos, type_id, type_name);
    }

    void call(const node_id &id, std::string_view fqn, std::span<const std::byte> param, typename procedure_forwarder<Policy>::on_response_fn on_response)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_procedures.call(session->rpc_peer(), fqn, param, std::move(on_response), std::nullopt, session->session_id());
    }

    void unsubscribe(const node_id &id, std::string_view fqn)
    {
        m_messages.forget_remembered_demand(node_name_of(id), fqn);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            session->unsubscribe(fqn);
    }

    void publish(std::string_view fqn, std::span<const std::byte> payload)
    {
        m_messages.publish(fqn, payload);
    }

    // This node's own topics fold into the same table its peers' edges do, under the identity the
    // handshake asserts to them — so a count, an enumeration or a by-participant view answers for
    // the whole graph rather than for everyone-but-me, and a party to a polytype disagreement can
    // see the disagreement it is party to. No wire verb rides this: a local edge is a local table
    // operation, and peers still learn the topic from the declare/subscribe they always did.
    void note_local_topic(std::string_view fqn, std::string_view type_name, std::optional<std::uint64_t> type_id, graph::topic_role role)
    {
        const bool changed = m_topics.upsert(graph::topic_edge{m_build.fsm_cfg.self_id, fqn, type_name, type_id, role}).changed;
        bump_graph_generation(changed, m_build.fsm_cfg.self_id, graph::change_kind::appeared);
    }

    void forget_local_topic(std::string_view fqn, graph::topic_role role)
    {
        const bool changed = m_topics.remove_edge(m_build.fsm_cfg.self_id, fqn, role);
        bump_graph_generation(changed, m_build.fsm_cfg.self_id, graph::change_kind::disappeared);
    }

    bool is_connected(const node_id &id) const
    {
        return m_registry.is_connected(id);
    }

    bool is_dead(const node_id &id) const
    {
        return m_registry.is_dead(id);
    }

    bool has_session(const node_id &id) const
    {
        return m_registry.has_session(id);
    }

    std::uint32_t attempt_count(const node_id &id) const
    {
        return m_registry.attempt_count(id);
    }

    session_type *session_for(const node_id &id)
    {
        return m_registry.session_for(id);
    }

    const basic_known_peers<PeerStorage> &known() const noexcept
    {
        return m_known_peers;
    }

    const graph::basic_topic_type_table<TopicStorage> &topic_table() const noexcept
    {
        return m_topics;
    }

    io::route_options route_opts() const noexcept
    {
        return m_route_options;
    }

    std::uint64_t graph_generation() const noexcept
    {
        return m_graph_generation;
    }

    std::size_t reported_origin_count() const noexcept
    {
        return m_emitter.reported_count();
    }

    // Reports dropped at the receiver's flood bound (a per-reporter origin ceiling or a per-origin
    // topic ceiling) — the honest drop accounting a bounded ingress keeps against a hostile relay.
    std::size_t reported_dropped_count() const noexcept
    {
        return m_report_dropped;
    }

    bool reports_origin(const node_id &origin) const noexcept
    {
        return m_emitter.reports(origin);
    }

    io::host_fingerprint local_fingerprint() const noexcept
    {
        return m_build.fsm_cfg.local_fingerprint;
    }

    message_forwarder<Policy> &messages() noexcept
    {
        return m_messages;
    }

    procedure_forwarder<Policy> &procedures() noexcept
    {
        return m_procedures;
    }

    registry_type &registry() noexcept
    {
        return m_registry;
    }

    upgrade_coordinator<registry_type, channel_type> &coordinator() noexcept
    {
        return m_coordinator;
    }

    void on_upgrade_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_coordinator.on_policy(std::move(policy));
    }

    void on_upgrade_gate(plexus::detail::move_only_function<upgrade_mint<channel_type>(std::string_view)> mint)
    {
        m_coordinator.on_gate(std::move(mint));
    }

    void on_upgrade_receive_gate(plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> mint)
    {
        m_coordinator.on_receive_gate(std::move(mint));
    }

    void inject_upgrade_receive(std::string_view node_name, std::span<const std::byte> frame)
    {
        if(session_type *s = m_registry.session_for_name(node_name))
            s->inject_receive(frame);
    }

    capture_policy &capture() noexcept
    {
        return m_capture;
    }

    void post_participant(const participant_event &ev)
    {
        Policy::post(m_executor, [this, ev] { detail::fan_out(*this, [&](observer &o) { o.on_participant(ev); }); });
    }

    void post_endpoint(std::string_view fqn, const endpoint_event &ev)
    {
        Policy::post(m_executor, [this, fqn = std::string{fqn}, ev] { detail::fan_out(*this, [&](observer &o) { o.on_endpoint(fqn, ev); }); });
    }

    void post_participant_teardown(const participant_event &ev)
    {
        Policy::post(m_executor,
                     [snapshot = m_observers, ev]
                     {
                         for(observer &o : snapshot)
                             o.on_participant(ev);
                     });
    }

private:
    bool m_dial_eagerly;
    io::liveliness_options m_liveliness;
    io::route_options m_route_options;
    io::report_universe_ctx m_report;
    Transport &m_transport;
    executor_type m_executor;
    capture_policy m_capture;
    security_fanout m_security_fanout;
    message_forwarder<Policy> m_messages;
    procedure_forwarder<Policy> m_procedures;
    session_build_context<Policy> m_build;
    registry_type m_registry;
    basic_known_peers<PeerStorage> m_known_peers;
    graph::basic_topic_type_table<TopicStorage> m_topics;
    std::set<node_id> m_reported;
    std::map<node_id, std::size_t> m_reporter_load;
    std::size_t m_report_dropped{0};
    std::uint64_t m_graph_generation{0};
    bool m_graph_wakeup_pending{false};
    std::size_t m_graph_subscribers{0};
    GraphChangeLog m_graph_log;
    PeerReportEmitter m_emitter;
    liveliness_monitor<Policy, Clock, typename LivelinessStorage::monitor> m_monitor;
    peer_liveliness<typename LivelinessStorage::arbiter> m_peer_liveliness;
    std::vector<std::reference_wrapper<observer>> m_observers;
    upgrade_coordinator<registry_type, channel_type> m_coordinator;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness_cb;

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
    friend void detail::post_edge(E &, const lifecycle_event &, void (observer::*)(const node_id &, std::string_view, peer_kind));

    template<typename E>
    friend void detail::post_rejected(E &, const lifecycle_event &);

    template<typename E>
    friend void detail::post_drop(E &, const detail::drop_event &);

    template<typename E>
    friend void detail::post_security(E &, const security_event &);

    template<typename E>
    friend void detail::post_liveliness(E &, const peer_liveliness_event &);

    template<typename E>
    friend void detail::post_wire(E &, recording::wire_direction, std::uint64_t, const node_id &, std::span<const std::byte>);

    bool demand_in_scope(const node_id &id, locality reach_mask) const
    {
        if(reach_mask == locality::any)
            return true;
        auto ep = m_known_peers.lookup(id);
        if(!ep)
            return false;
        return any_set(reach_mask, tier_of(ep->scheme));
    }

    bool reliability_in_scope(const node_id &id, reliability_requirement require) const
    {
        if(require == reliability_requirement::any)
            return true;
        auto ep = m_known_peers.lookup(id);
        if(!ep)
            return false;
        return scheme_is_reliable(ep->scheme);
    }

    void on_dialed(std::unique_ptr<channel_type> channel, const endpoint &dialed)
    {
        wire_channel_drop(*channel);
        wire_channel_capture(*channel, m_registry.id_for_endpoint(dialed).value_or(node_id{}));
        m_registry.build_session_for_endpoint(dialed, std::move(channel));
    }

    void on_accepted(std::unique_ptr<channel_type> channel)
    {
        wire_channel_drop(*channel);
        m_registry.accept_session(std::move(channel), [this](channel_type &ch, const node_id &peer) { wire_channel_capture(ch, peer); });
    }

    void wire_channel_drop(channel_type &channel)
    {
        if constexpr(requires { channel.on_drop(detail::make_drop_sink(*this)); })
            channel.on_drop(detail::make_drop_sink(*this));
    }

    void wire_channel_capture(channel_type &channel, const node_id &peer)
    {
        if constexpr(requires { channel.on_wire(detail::make_wire_sink(*this, peer)); })
            channel.on_wire(detail::make_wire_sink(*this, peer));
    }

    // A rejected handshake is an un-established session: tear_down's prior-complete guard suppresses
    // the disconnected edge, so a dead/disconnected feed is NOT guaranteed to follow. Mapping rejected
    // to session-down settles the peer as not-alive rather than leaving a stuck session state.
    void feed_liveliness_session(const lifecycle_event &ev)
    {
        switch(ev.edge)
        {
            case lifecycle_edge::ready:
                register_endpoint(ev.id, ev.node_name);
                return m_peer_liveliness.note_session_up(ev.id);
            case lifecycle_edge::connected:
            case lifecycle_edge::reconnected:
                return m_peer_liveliness.note_session_up(ev.id);
            case lifecycle_edge::disconnected:
            case lifecycle_edge::dead:
                m_monitor.deregister_endpoint(ev.id);
                m_coordinator.on_peer_dead(ev.node_name);
                return m_peer_liveliness.note_session_down(ev.id, now_for_aging());
            case lifecycle_edge::rejected:
                return m_peer_liveliness.note_session_down(ev.id, now_for_aging());
        }
    }

    // A peer that both dials and accepts holds two sessions under two slot keys but one proven
    // identity, and the table is keyed by that identity.
    bool any_session_for(const node_id &proven)
    {
        bool found = false;
        m_registry.for_each_connected([&](const node_id &, session_type &s) { found = found || s.peer_identity() == proven; });
        return found;
    }

    // Topic knowledge is scoped to the sessions that carried it, so a peer's records leave with the
    // last of them — mirroring how the registry drops its fan-out entries. Awareness is untouched:
    // a peer that goes down stays discovered, it just stops asserting topics.
    void forget_topics_on_teardown(const lifecycle_event &ev)
    {
        if(ev.edge != lifecycle_edge::disconnected && ev.edge != lifecycle_edge::dead)
            return;
        session_type *torn = m_registry.session_for(ev.id);
        if(torn != nullptr && !any_session_for(torn->peer_identity()))
        {
            const node_id who  = torn->peer_identity();
            relay_withdraw(who);
            retire_reports_via(who);
            const bool changed = m_topics.remove_node(who);
            bump_graph_generation(changed, who, graph::change_kind::disappeared);
        }
    }

    void dispatch_lifecycle(const lifecycle_event &ev)
    {
        feed_liveliness_session(ev);
        forget_topics_on_teardown(ev);
        if(ev.edge == lifecycle_edge::connected || ev.edge == lifecycle_edge::reconnected)
        {
            reset_windows_for_reporter(ev.id);
            relay_on_ready(ev.id);
        }
        switch(ev.edge)
        {
            case lifecycle_edge::connected:
                return detail::post_edge(*this, ev, &observer::on_peer_connected);
            case lifecycle_edge::disconnected:
                return detail::post_edge(*this, ev, &observer::on_peer_disconnected);
            case lifecycle_edge::reconnected:
                return detail::post_edge(*this, ev, &observer::on_peer_reconnected);
            case lifecycle_edge::dead:
                return detail::post_edge(*this, ev, &observer::on_peer_dead);
            case lifecycle_edge::ready:
                return detail::post_edge(*this, ev, &observer::on_peer_ready);
            case lifecycle_edge::rejected:
                return detail::post_rejected(*this, ev);
        }
    }

    void register_endpoint(const node_id &id, const std::string &node_name)
    {
        for(const auto &demand : m_messages.remembered_topics(node_name))
            m_monitor.register_endpoint(id, wire::fqn_topic_hash(demand.fqn), demand.qos.requested_deadline_ns, demand.qos.requested_lease_ns);
    }

    void fan_liveness(const liveness_event &ev)
    {
        if(m_on_liveness_cb)
            m_on_liveness_cb(ev);
    }

    std::uint64_t now_for_aging() const
    {
        return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
    }

    // Level-triggered coalescing: a real change bumps a monotonic generation and, only while a graph
    // subscriber is registered, appends the {who, kind} delta; a flood collapses to ONE posted wakeup
    // because a second bump with one already in flight only advances the counter. The fan-out is the
    // deferred post — never synchronous from this mutator body or a teardown frame (the DoS-amplifier
    // and freed-slot_block guards). An idempotent mutation reports changed==false and is silent.
    void bump_graph_generation(bool changed, const node_id &who, graph::change_kind kind)
    {
        if(!changed)
            return;
        ++m_graph_generation;
        if(m_graph_subscribers != 0)
            m_graph_log.append(who, kind);
        if(m_graph_wakeup_pending)
            return;
        m_graph_wakeup_pending = true;
        Policy::post(m_executor, [this] { fire_graph_wakeup(); });
    }

    // The posted wakeup: fires the coarse edge once at the FINAL generation, then drains the host
    // delta log to the opt-in observers. Iterates the live observer list directly rather than
    // detail::fan_out — the snapshot copy fan_out takes would allocate on every wakeup, and the
    // coarse path must stay allocation-free (the [this] closure fits move_only_function's SBO). It
    // re-reads known_peers/topic_table only, never slot_block, so a peer torn down before the drain
    // cannot be re-entered.
    void fire_graph_wakeup()
    {
        m_graph_wakeup_pending = false;
        const std::uint64_t gen = m_graph_generation;
        for(std::size_t i = 0; i < m_observers.size(); ++i)
            m_observers[i].get().on_graph_changed(gen);
        const auto deltas = m_graph_log.drain();
        if(deltas.empty())
            return;
        for(std::size_t i = 0; i < m_observers.size(); ++i)
        {
            observer &o = m_observers[i].get();
            if(o.observes_graph())
                for(const graph::graph_change &delta : deltas)
                    o.on_graph_delta(delta);
        }
        m_graph_log.clear();
    }

    // The reported-candidate note path (hop budget + per-origin dedup, then write). A withdrawal-
    // flagged report retires instead — a removal is never rate-limited. Only a first sighting bumps
    // the graph appeared; a fresh re-report refreshes silently, a duplicate/over-budget report is a
    // no-op. No dial and no direct-liveliness note: a reported identity is via-only.
    void note_reported_candidate(const node_id &reporter, const wire::peer_report &pr)
    {
        if(pr.flags & wire::k_peer_report_withdrawal_flag)
            return retire_reported_if_fresh(pr.origin, reporter, pr.seq);
        if(pr.hop > m_report.hop_budget)
            return;
        if(pr.topics.size() > m_report.max_report_topics || reporter_at_capacity(reporter, pr.origin))
            return (void)++m_report_dropped;
        const auto oc = m_known_peers.note_reported(pr.origin, reported_candidate(reporter, pr), pr.seq, now_for_aging(), m_route_options);
        if(oc != detail::report_admit::noted_new && oc != detail::report_admit::refreshed)
            return;
        fold_report_topics(pr);
        if(oc == detail::report_admit::noted_new)
        {
            ++m_reporter_load[reporter];
            m_reported.insert(pr.origin);
            bump_graph_generation(true, pr.origin, graph::change_kind::appeared);
        }
    }

    // A reporter may install at most max_reported_origins DISTINCT origins: a report that would add a
    // NEW (origin, via=reporter) row past the ceiling is rejected and counted, never evicting a held
    // row and never touching a direct peer. A refresh of an origin the reporter already reaches passes.
    bool reporter_at_capacity(const node_id &reporter, const node_id &origin) const
    {
        auto it = m_reporter_load.find(reporter);
        if(it == m_reporter_load.end() || it->second < m_report.max_reported_origins)
            return false;
        return !has_row_via(origin, reporter);
    }

    bool has_row_via(const node_id &origin, const node_id &via) const
    {
        for(const route_candidate &c : m_known_peers.candidates(origin))
            if(!c.is_direct() && c.reach.via == via)
                return true;
        return false;
    }

    void dec_reporter_load(const node_id &reporter)
    {
        auto it = m_reporter_load.find(reporter);
        if(it != m_reporter_load.end() && --it->second == 0)
            m_reporter_load.erase(it);
    }

    // Release the per-reporter load of every transitive row a record holds, before the whole record
    // is removed (a TTL sweep or a forget) rather than retired one via at a time.
    void release_all_reporter_load(const node_id &origin)
    {
        for(const route_candidate &c : m_known_peers.candidates(origin))
            if(!c.is_direct() && c.reach.via)
                dec_reporter_load(*c.reach.via);
    }

    // The relay R is gone: retire every origin still reported as reachable via R (R can no longer
    // withdraw them itself), driving the same retire path a withdrawal does. Gather-then-retire so the
    // per-origin removal never mutates the table mid-iteration.
    void retire_reports_via(const node_id &via)
    {
        std::vector<node_id> origins;
        m_known_peers.for_each_origin_via(via, [&](const node_id &o) { origins.push_back(o); });
        for(const node_id &o : origins)
            retire_reported(o, via);
    }

    route_candidate reported_candidate(const node_id &reporter, const wire::peer_report &pr) const
    {
        route_candidate cand{};
        // A via-only row carries a default-constructed (empty-scheme) endpoint as reach.transport: it
        // is reachability hearsay, never a dial target. The engine resolves dials through the DIRECT
        // endpoint only (route_admission::direct_endpoint), so this row is structurally non-dialable.
        cand.reach      = graph::route{endpoint{}, reporter};
        cand.origin     = graph::provenance{graph::observation::reported, reporter};
        cand.hop        = pr.hop;
        cand.seq_window = wire::udp_dedup_window{m_report.dedup_depth};
        return cand;
    }

    // A reported origin has no session, so its topic edges must be folded here and retired on
    // withdrawal/aging or they outlive the reachability that carried them. The origin's wire
    // declarations fold under its own id as publisher edges.
    void fold_report_topics(const wire::peer_report &pr)
    {
        for(const wire::topic_declaration &td : pr.topics)
        {
            const std::optional<std::uint64_t> tid = td.state == wire::type_state::undeclared ? std::nullopt : std::optional{td.type_id};
            const bool changed = m_topics.upsert(graph::topic_edge{pr.origin, td.fqn, td.type_name, tid, graph::topic_role::publisher}).changed;
            bump_graph_generation(changed, pr.origin, graph::change_kind::appeared);
        }
    }

    // Retires a reported origin's transitive row and, once no reachability remains, its topic edges —
    // WITHOUT churning the identity (only reachability rows retire). A row still held by a direct
    // session keeps both the id and its topics.
    void retire_reported(const node_id &origin, const node_id &via)
    {
        if(!m_known_peers.remove_transitive(origin, via))
            return;
        dec_reporter_load(via);
        if(m_known_peers.contains(origin))
            return;
        m_reported.erase(origin);
        m_topics.remove_node(origin);
        bump_graph_generation(true, origin, graph::change_kind::disappeared);
    }

    // A withdrawal retires the origin's row via `via` only when its seq is fresh against that row's
    // dedup window: a reordered or replayed stale withdrawal (a hostile relay alternating
    // assert/withdraw, or a UDP-backed reorder) is dropped, so it cannot retire a re-asserted live row.
    void retire_reported_if_fresh(const node_id &origin, const node_id &via, std::uint16_t seq)
    {
        if(!m_known_peers.withdraw_seq_fresh(origin, via, seq))
            return;
        retire_reported(origin, via);
    }

    // A reporter's session (re)completed: re-arm the dedup windows of the rows it feeds so its next
    // report re-anchors on its current (possibly restarted-from-0) seq counter instead of deduping
    // against a stale high-water mark from the prior incarnation.
    void reset_windows_for_reporter(const node_id &id)
    {
        if(session_type *s = m_registry.session_for(id))
            m_known_peers.reset_reported_windows(s->peer_identity());
    }

    // Re-assert every held report on the relay's heartbeat cadence so a downstream row for a
    // still-live-but-idle origin refreshes before it ages out at awareness_ttl. Relay-only: the null
    // emitter twin makes this a no-op, so a non-relay node still emits nothing.
    void relay_reassert()
    {
        m_emitter.reassert([this](const wire::peer_report &pr) { relay_broadcast(pr); });
    }

    // The emit seam a newly-completed session runs: hand it every origin already reported (skipping a
    // report about its own peer) the way redeclare_all replays declarations, then lift the just-
    // attached peer as an origin and assert it to the rest. A non-relay node threads the null twin, so
    // both calls are no-ops and no peer_report leaves the wire.
    void relay_on_ready(const node_id &id)
    {
        session_type *fresh = m_registry.session_for(id);
        if(fresh == nullptr)
            return;
        const node_id peer = fresh->peer_identity();
        m_emitter.replay(peer, [this, fresh](const wire::peer_report &pr) { relay_send(*fresh, pr); });
        relay_note_origin(peer);
    }

    // Re-lift an already-attached origin whose topic table changed, so a late or updated declaration
    // re-announces downstream. Only a directly-attached origin (never self, never a via-only reported
    // candidate) is a relay source.
    void relay_maybe_refresh(const node_id &node)
    {
        if(node == m_build.fsm_cfg.self_id || !any_session_for(node))
            return;
        relay_note_origin(node);
    }

    void relay_note_origin(const node_id &origin)
    {
        m_emitter.note_origin(m_report, origin, m_report.universe, m_topics, [this](const wire::peer_report &pr) { relay_broadcast(pr); });
    }

    void relay_withdraw(const node_id &origin)
    {
        m_emitter.withdraw(origin, [this](const wire::peer_report &pr) { relay_broadcast(pr); });
    }

    // Send one report to every connected session but the origin's own — an origin is never announced
    // back to itself. The encode is per-broadcast, off the emitted report only.
    void relay_broadcast(const wire::peer_report &pr)
    {
        auto bytes = wire::encode_peer_report(pr);
        m_registry.for_each_connected(
                [&](const node_id &, session_type &s)
                {
                    if(s.peer_identity() != pr.origin)
                        m_messages.send_peer_report(s.msg_peer().channel, bytes);
                });
    }

    void relay_send(session_type &s, const wire::peer_report &pr)
    {
        if(s.peer_identity() != pr.origin)
            m_messages.send_peer_report(s.msg_peer().channel, wire::encode_peer_report(pr));
    }

    // Awareness-only removal on the existing tick: forgets a peer that stopped re-announcing and was
    // not refreshed by a heartbeat. A reported origin ages out the same way — its topics retire with
    // it (a live reporter is the only refresh). NEVER touches m_registry — an active session outlives
    // its awareness entry.
    void sweep_aged_awareness()
    {
        const std::uint64_t now      = now_for_aging();
        const std::uint64_t ttl      = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(m_liveliness.awareness_ttl).count());
        const std::uint64_t deadline = now > ttl ? now - ttl : 0;
        m_known_peers.expire_older_than(deadline,
                                        [this](const node_id &id)
                                        {
                                            release_all_reporter_load(id);
                                            m_peer_liveliness.note_awareness_lost(id);
                                            relay_withdraw(id);
                                            if(m_reported.erase(id) != 0)
                                                m_topics.remove_node(id);
                                            bump_graph_generation(true, id, graph::change_kind::disappeared);
                                        });
    }
};

}

#endif

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
#include "plexus/io/route_select.h"
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

#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/routing_close.h"
#include "plexus/io/detail/routing_sinks.h"
#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/routing_reports.h"
#include "plexus/io/detail/routing_dispatch.h"
#include "plexus/io/detail/routing_lifecycle.h"
#include "plexus/io/detail/routing_relay_emit.h"
#include "plexus/io/detail/routing_forward_rpc.h"
#include "plexus/io/detail/routing_graph_change.h"
#include "plexus/io/detail/routing_relay_demand.h"
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
         typename GraphChangeLog = graph::vector_graph_change_log, typename PeerReportEmitter = null_peer_report_emitter, typename ForwardSplice = null_forward_splice>
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
    using forward_splice_type      = ForwardSplice;
    using session_type       = peer_session<Policy>;
    using executor_type      = typename Policy::executor_type;
    using channel_type       = typename Policy::byte_channel_type;
    using registry_type      = peer_session_registry<Policy, Transport, Clock>;

    routing_engine(Transport &transport, executor_type executor, const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout, const reconnect_config &redial,
                   std::uint64_t redial_seed, log::logger &logger, bool dial_eagerly = false, std::size_t global_default = io::global_default_max_message_bytes,
                   io::liveliness_options live = {}, io::route_options routes = {}, const io::report_options &report = {}, const io::forward_options &forward = {})
            : m_dial_eagerly(dial_eagerly)
            , m_liveliness(live)
            , m_route_options(routes)
            , m_report(io::make_report_ctx(report))
            , m_transport(transport)
            , m_executor(executor)
            , m_security_fanout{*this}
            , m_messages(logger, global_default)
            , m_procedures(executor, handshake_timeout, logger)
            , m_build{executor, fsm_cfg, handshake_timeout, m_messages, m_procedures, redial, redial_seed, logger, {}, {}, {}, {}, routes.usage == route_usage::never, {}, m_security_fanout, {}, {}}
            , m_ops()
            , m_registry(transport, m_build)
            , m_monitor(m_executor)
            , m_peer_liveliness(m_liveliness)
            , m_coordinator(m_registry)
            , m_retry_timer(executor)
            , m_splice(io::make_forward_ctx(forward))
    {
        m_messages.set_forward_options(forward);
        m_messages.set_consume_relayed(routes.usage != route_usage::never);
        detail::install_routing_sinks(*this);
        detail::install_upgrade_retry(m_coordinator, m_retry_timer);
        detail::install_splice_refan(*this);
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

    void hang_up(const endpoint &ep)
    {
        detail::hang_up_endpoint(*this, ep);
    }

    // Detaches from the transport, then closes every slot and the transport without arming a redial.
    void close()
    {
        detail::close_engine(*this);
    }

    bool closing() const noexcept
    {
        return m_ops.closing();
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
    // suggestion reach() gates on, so an in-flight or armed reconnect for the peer is canceled
    // rather than left to resurrect it. An established session is untouched — it ends on its own
    // teardown, not this call.
    void forget(const node_id &id)
    {
        detail::release_all_reporter_load(*this, id);
        const bool changed = m_known_peers.forget(id);
        m_registry.deny_redial(id);
        m_peer_liveliness.note_awareness_lost(id);
        detail::relay_withdraw(*this, id);
        m_emitter.clear_decline(id);
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
        detail::note_reported_candidate(*this, reporter, pr);
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

    void call(const node_id &id, std::string_view fqn, std::span<const std::byte> param, typename procedure_forwarder<Policy>::on_response_fn on_response,
              std::optional<std::chrono::nanoseconds> deadline = std::nullopt)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_procedures.call(session->rpc_peer(), fqn, param, std::move(on_response), deadline, session->session_id());
        else
            m_procedures.forward_call(id, fqn, param, std::move(on_response), deadline);
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

    // Re-point the node's route-usage knob at runtime: the single switch drives the local relayed-use
    // gate AND the cooperative decline posture stamped on every live session's next heartbeat, so an
    // un-decline restores lift and offer for free on the level carrier.
    void set_route_usage(route_usage usage)
    {
        m_route_options.usage  = usage;
        const bool declines    = usage == route_usage::never;
        m_build.declines_relay = declines;
        m_messages.set_consume_relayed(!declines);
        m_registry.for_each_connected([declines](const node_id &, session_type &s) { s.set_declines_relay(declines); });
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

    // Whether a peer has asserted its cooperative decline posture — the relay skips offering it any
    // relayed path. Always false on a non-relay node's null twin.
    bool peer_declines(const node_id &peer) const noexcept
    {
        return m_emitter.declines(peer);
    }

    // Forwarded request/response frames dropped for want of a route (route_select over empty candidates,
    // or a chosen via with no live session) — the honest count behind a reply-fail that no route is
    // manufactured to paper over.
    std::size_t forward_rpc_dropped_count() const noexcept
    {
        return m_forward_rpc_dropped;
    }

    // How often a same-host lane was declined, demoted or lost, indexed by fallback_reason.
    fallback_counts same_host_fallbacks() const noexcept
    {
        return m_coordinator.fallbacks();
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
    io::detail::pending_ops m_ops;
    registry_type m_registry;
    basic_known_peers<PeerStorage> m_known_peers;
    graph::basic_topic_type_table<TopicStorage> m_topics;
    std::set<node_id> m_reported;
    std::map<node_id, std::size_t> m_reporter_load;
    std::size_t m_report_dropped{0};
    std::size_t m_forward_rpc_dropped{0};
    std::uint16_t m_forward_rpc_seq{0};
    std::vector<std::byte> m_forward_rpc_scratch;
    std::uint64_t m_graph_generation{0};
    bool m_graph_wakeup_pending{false};
    std::size_t m_graph_subscribers{0};
    GraphChangeLog m_graph_log;
    PeerReportEmitter m_emitter;
    liveliness_monitor<Policy, Clock, typename LivelinessStorage::monitor> m_monitor;
    peer_liveliness<typename LivelinessStorage::arbiter> m_peer_liveliness;
    std::vector<std::reference_wrapper<observer>> m_observers;
    upgrade_coordinator<registry_type, channel_type> m_coordinator;
    typename Policy::timer_type m_retry_timer;
    plexus::detail::move_only_function<void(const liveness_event &)> m_on_liveness_cb;
    ForwardSplice m_splice;

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

    template<typename E>
    friend void detail::note_reported_candidate(E &, const node_id &, const wire::peer_report &);

    template<typename E>
    friend void detail::recover_reachable_via(E &, const node_id &, const node_id &);

    template<typename E>
    friend bool detail::reporter_at_capacity(const E &, const node_id &, const node_id &);

    template<typename E>
    friend bool detail::has_row_via(const E &, const node_id &, const node_id &);

    template<typename E>
    friend void detail::dec_reporter_load(E &, const node_id &);

    template<typename E>
    friend void detail::release_all_reporter_load(E &, const node_id &);

    template<typename E>
    friend void detail::mark_reports_via_unreachable(E &, const node_id &);

    template<typename E>
    friend route_candidate detail::reported_candidate(const E &, const node_id &, const wire::peer_report &);

    template<typename E>
    friend void detail::fold_report_topics(E &, const wire::peer_report &);

    template<typename E>
    friend void detail::retire_reported(E &, const node_id &, const node_id &);

    template<typename E>
    friend void detail::retire_reported_if_fresh(E &, const node_id &, const node_id &, std::uint16_t);

    template<typename E>
    friend void detail::reset_windows_for_reporter(E &, const node_id &);

    template<typename E>
    friend bool detail::any_session_for(E &, const node_id &);

    template<typename E>
    friend void detail::register_endpoint(E &, const node_id &, const std::string &);

    template<typename E>
    friend void detail::feed_liveliness_session(E &, const lifecycle_event &);

    template<typename E>
    friend void detail::forget_topics_on_teardown(E &, const lifecycle_event &);

    template<typename E>
    friend void detail::arm_relayed_suppression(E &, const node_id &);

    template<typename E>
    friend void detail::dispatch_lifecycle(E &, const lifecycle_event &);

    template<typename E>
    friend void detail::fan_liveness(E &, const liveness_event &);

    template<typename E>
    friend typename E::session_type *detail::session_by_identity(E &, const node_id &);

    template<typename E>
    friend typename E::session_type *detail::forward_session_for(E &, std::span<const route_candidate>, std::size_t, const node_id &);

    template<typename E>
    friend void detail::send_forwarded_rpc(E &, typename E::session_type &, const node_id &, const node_id &, std::uint8_t, std::span<const std::byte>);

    template<typename E>
    friend bool detail::forward_rpc(E &, const node_id &, const node_id &, std::uint8_t, std::span<const std::byte>);

    template<typename E>
    friend void detail::relay_send(E &, typename E::session_type &, const wire::peer_report &);

    template<typename E>
    friend void detail::relay_broadcast(E &, const wire::peer_report &);

    template<typename E>
    friend void detail::relay_note_origin(E &, const node_id &);

    template<typename E>
    friend void detail::relay_withdraw(E &, const node_id &);

    template<typename E>
    friend void detail::relay_reassert(E &);

    template<typename E>
    friend void detail::relay_on_ready(E &, const node_id &);

    template<typename E>
    friend void detail::relay_maybe_refresh(E &, const node_id &);

    template<typename E>
    friend void detail::note_decline(E &, const node_id &, bool);

    template<typename E>
    friend bool detail::publishes_topic(const E &, const node_id &, std::string_view);

    template<typename E>
    friend void detail::relay_propagate_demand(E &, std::string_view, demand_transition, demand_role);

    template<typename E>
    friend void detail::install_splice_refan(E &);

    template<typename E>
    friend void detail::close_engine(E &);

    template<typename E>
    friend void detail::hang_up_endpoint(E &, const endpoint &);

    template<typename E>
    friend void detail::fire_graph_wakeup(E &);

    template<typename E>
    friend void detail::sweep_aged_awareness(E &);

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
        Policy::post(m_executor, [this] { detail::fire_graph_wakeup(*this); });
    }
};

}

#endif

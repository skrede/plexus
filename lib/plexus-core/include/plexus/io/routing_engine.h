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
#include "plexus/io/liveliness_options.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/upgrade_coordinator.h"
#include "plexus/io/peer_session_registry.h"
#include "plexus/io/session_build_context.h"
#include "plexus/io/reliability_requirement.h"

#include "plexus/graph/topic_type_table.h"

#include "plexus/io/detail/routing_sinks.h"
#include "plexus/io/detail/routing_dispatch.h"
#include "plexus/io/detail/routing_sink_install.h"

#include "plexus/log/logger.h"

#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

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
         typename LivelinessStorage = default_liveliness_storage>
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
    using policy_type   = Policy;
    using session_type  = peer_session<Policy>;
    using executor_type = typename Policy::executor_type;
    using channel_type  = typename Policy::byte_channel_type;
    using registry_type = peer_session_registry<Policy, Transport, Clock>;

    routing_engine(Transport &transport, executor_type executor, const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout, const reconnect_config &redial,
                   std::uint64_t redial_seed, log::logger &logger, bool dial_eagerly = false, std::size_t global_default = io::global_default_max_message_bytes,
                   io::liveliness_options live = {})
            : m_dial_eagerly(dial_eagerly)
            , m_liveliness(live)
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
    }

    void remove_observer(observer &o)
    {
        const auto erased = std::erase_if(m_observers, [&](const std::reference_wrapper<observer> &w) { return &w.get() == &o; });
        if(erased != 0 && o.observes_data_path())
            m_capture.remove_observer();
        if(erased != 0 && o.observes_liveliness())
            m_peer_liveliness.remove_subscriber();
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
        m_known_peers.note_peer(id, ep, now);
        m_peer_liveliness.note_awareness(id, now);
        if(m_dial_eagerly)
            reach(id);
    }

    // Removes a peer's awareness only (a goodbye drops the suggestion, never an active link):
    // mirrors note_peer's awareness scope and never touches the session registry.
    void forget(const node_id &id)
    {
        m_known_peers.forget(id);
        m_peer_liveliness.note_awareness_lost(id);
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

    const graph::topic_type_table &topic_table() const noexcept
    {
        return m_topics;
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
    Transport &m_transport;
    executor_type m_executor;
    capture_policy m_capture;
    security_fanout m_security_fanout;
    message_forwarder<Policy> m_messages;
    procedure_forwarder<Policy> m_procedures;
    session_build_context<Policy> m_build;
    registry_type m_registry;
    basic_known_peers<PeerStorage> m_known_peers;
    graph::topic_type_table m_topics;
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

    // Topic knowledge is scoped to the session that carried it, so a peer's records leave with its
    // session — mirroring how the registry drops its fan-out entries. Awareness is untouched: a
    // peer that goes down stays discovered, it just stops asserting topics.
    void forget_topics_on_teardown(const lifecycle_event &ev)
    {
        if(ev.edge == lifecycle_edge::disconnected || ev.edge == lifecycle_edge::dead)
            m_topics.remove_node(ev.id);
    }

    void dispatch_lifecycle(const lifecycle_event &ev)
    {
        feed_liveliness_session(ev);
        forget_topics_on_teardown(ev);
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

    // Awareness-only removal on the existing tick: forgets a peer that stopped re-announcing and was
    // not refreshed by a heartbeat. NEVER touches m_registry — an active session outlives its
    // awareness entry.
    void sweep_aged_awareness()
    {
        const std::uint64_t now      = now_for_aging();
        const std::uint64_t ttl      = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(m_liveliness.awareness_ttl).count());
        const std::uint64_t deadline = now > ttl ? now - ttl : 0;
        m_known_peers.expire_older_than(deadline, [this](const node_id &id) { m_peer_liveliness.note_awareness_lost(id); });
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H
#define HPP_GUARD_PLEXUS_IO_ROUTING_ENGINE_H

#include "plexus/io/known_peers.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include <deque>
#include <chrono>
#include <memory>
#include <string>
#include <cstdint>
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
        , m_procedures(executor, handshake_timeout, logger)
        , m_build{executor, fsm_cfg, handshake_timeout, m_messages, m_procedures,
                  redial, redial_seed, logger}
        , m_registry(transport, m_build)
        , m_dial_eagerly(dial_eagerly)
    {
        m_transport.on_dialed([this](std::unique_ptr<channel_type> ch) { on_dialed(std::move(ch)); });
        m_transport.on_accepted([this](std::unique_ptr<channel_type> ch) { on_accepted(std::move(ch)); });
        m_transport.on_dial_failed([this](io_error) { on_dial_failed(); });
        m_registry.on_slot_redial([this](const node_id &id) { m_pending_dials.push_back(id); });
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
        m_registry.ensure_slot(id, *ep, node_name_for(id));
        m_registry.driver_for(id).start();
    }

    // A demand verb: reach the named peer, then attach the topic through the owned
    // message_forwarder once connected (the attach is what makes a publish flow).
    void subscribe(const node_id &id, std::string_view fqn)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_messages.attach(session->msg_peer(), fqn);
    }

    // A demand verb: reach then call through the owned procedure_forwarder.
    void call(const node_id &id, std::string_view fqn, std::span<const std::byte> param,
              typename procedure_forwarder<Policy>::on_response_fn on_response)
    {
        reach(id);
        auto *session = m_registry.session_for(id);
        if(session != nullptr && session->is_complete())
            m_procedures.call(session->rpc_peer(), fqn, param, std::move(on_response),
                              std::nullopt, session->session_id());
    }

    // PUBLISH does NOT dial: it fans through the message_forwarder to whoever is
    // already subscribed and triggers NO reach (the demand is the remote subscribe).
    void publish(std::string_view fqn, std::span<const std::byte> payload)
    {
        m_messages.publish(fqn, payload);
    }

    bool is_connected(const node_id &id) const { return m_registry.is_connected(id); }
    bool has_session(const node_id &id) const { return m_registry.has_session(id); }
    std::uint32_t attempt_count(const node_id &id) const { return m_registry.attempt_count(id); }
    session_type *session_for(const node_id &id) { return m_registry.session_for(id); }

    const known_peers &known() const noexcept { return m_known; }
    message_forwarder<Policy> &messages() noexcept { return m_messages; }
    procedure_forwarder<Policy> &procedures() noexcept { return m_procedures; }
    registry_type &registry() noexcept { return m_registry; }

private:
    // The single dial-success tail: the channel belongs to the oldest pending dial
    // target (FIFO over the in-flight dials). Hand it to the registry's shared
    // build-from-record -> start() tail.
    void on_dialed(std::unique_ptr<channel_type> channel)
    {
        if(m_pending_dials.empty())
            return;
        auto id = m_pending_dials.front();
        m_pending_dials.pop_front();
        m_registry.build_session(id, std::move(channel));
    }

    void on_accepted(std::unique_ptr<channel_type> channel)
    {
        m_registry.accept_session(std::move(channel));
    }

    // A refused dial drives the slot's own reconnect driver (the per-slot redial),
    // which the registry already wired in ensure_slot. The driver consumed the
    // failure through its own on_dial_failed wiring in start(); nothing set-wide.
    void on_dial_failed() {}

    std::string node_name_for(const node_id &id) const
    {
        std::string name = "peer-";
        name += std::to_string(static_cast<unsigned>(std::to_integer<std::uint8_t>(id[0])));
        return name;
    }

    Transport &m_transport;
    executor_type m_executor;
    message_forwarder<Policy> m_messages;
    procedure_forwarder<Policy> m_procedures;
    session_build_context<Policy> m_build;
    registry_type m_registry;
    known_peers m_known;
    std::deque<node_id> m_pending_dials;
    bool m_dial_eagerly;
};

}

#endif

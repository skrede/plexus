#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_H

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/frame_router.h"
#include "plexus/io/host_identity.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/security_event.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/attach_negotiator.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_consumers.h"
#include "plexus/io/detail/peer_session_handshake.h"

#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/stream_inbound.h"
#include "plexus/wire/fetch_latched.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::io {

// over-limit: one cohesive per-peer bridge; the public seam setters and the
// receive/staleness/teardown lifecycle share the borrowed ctx/negotiator/fsm + the per-
// incarnation counters and seam members, so splitting the surface scatters that shared state
// (the FSM-drive, handshake-encode, consumer-register, and deliver helpers are extracted to
// detail/peer_session_*.h).
//
// The per-peer bridge: one live connection turned into a session. It drives the pure
// handshake_fsm over an ALREADY-LIVE channel (the ctor performs no dial or listen so a
// future transport backend slots in unchanged), owns a per-peer frame_router whose
// consumers decode handshake frames into the FSM and hand data frames to the node-shared
// forwarders carrying THIS peer's identity, mints a per-connection session_id epoch on
// completion by drawing the next value from the per-peer record's epoch_source (the record
// OUTLIVES the incarnation, so a reconnect's fresh session is a strictly-later epoch), and
// runs the receive-path staleness gate. Re-entrancy safety is structural: on_data is always
// posted and the lifetime is single-owner, so the bridge needs no synchronization wrapper.
//
// The FSM-driving, install-glue, handshake-encode, consumer-register, and deliver helpers
// are extracted to detail/peer_session_*.h (relocation by friend); the irreducible session
// core — lifecycle wiring, the receive/staleness gate, the seams, teardown — stays here.
template<typename Policy>
    requires plexus::Policy<Policy>
class peer_session
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;

    // The per-peer record bundles the per-incarnation DATA the session draws from: ctx OWNS
    // the live channel (the session borrows it), the peer node_name, and the epoch well. The
    // record OUTLIVES this incarnation. handshake_timeout is required with NO default — a
    // session cannot arm a bound it was not told, and the value depends on the deployment.
    peer_session(peer_context<Policy> &ctx, executor_type executor,
                 const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout,
                 message_forwarder<Policy> &messages, procedure_forwarder<Policy> &procedures,
                 bool is_inbound_bootstrap, log::logger &logger = shared_null_logger())
            : m_ctx(ctx)
            , m_channel(*ctx.channel)
            , m_fsm_cfg(fsm_cfg)
            , m_fsm(fsm_cfg)
            , m_handshake_timer(executor)
            , m_messages(messages)
            , m_procedures(procedures)
            , m_handshake_timeout(handshake_timeout)
            , m_is_inbound_bootstrap(is_inbound_bootstrap)
            , m_from_intra_process(tier_of(m_channel.remote_endpoint().scheme) == locality::process)
            , m_logger(logger)
            , m_msg_peer{m_channel, ctx.node_name}
            , m_rpc_peer{m_channel, ctx.node_name}
    {
    }

    // Wire the receive path, register the router consumers, arm the handshake timer, and
    // (for an outbound peer) begin the handshake. Two paired sessions both begin outbound:
    // the simultaneous-connect path completes on both ends.
    // NOLINTNEXTLINE(readability-function-size)
    void start()
    {
        m_torn_down                 = false;
        m_closed_for_protocol_error = false;
        m_negotiator.prime();
        m_channel.on_data([this](std::span<const std::byte> f) { on_receive(f); });
        m_channel.on_error(
                [this](io_error e)
                {
                    // would_block is a TRANSIENT back-pressure stall (a full write queue under
                    // congestion=block), NOT a broken channel; tearing the session down on it
                    // would drop a live connection mid-transfer. Only a genuine channel break
                    // drives the transport-drop reconnect.
                    if(e == io_error::would_block)
                        return;
                    if(m_on_drop && !m_torn_down && !m_closed_for_protocol_error)
                        m_on_drop();
                });
        m_channel.on_protocol_close([this](wire::close_cause cause)
                                    { on_channel_protocol_close(cause); });
        // The process-tier object lane, compiled only when the concrete channel exposes it
        // (the inproc lane). A remote channel structurally lacks on_object.
        if constexpr(requires(channel_type &c) { c.on_object([](const object_carrier &) {}); })
            m_channel.on_object([this](const object_carrier &c)
                                { detail::deliver_session_object(*this, c); });
        detail::register_session_consumers(*this);
        arm_handshake_timer();
        if(!m_is_inbound_bootstrap)
        {
            detail::execute(*this, m_fsm.on_dial_started());
            detail::execute(*this, m_fsm.on_outbound_connected());
        }
    }

    template<typename OnMessage>
    void on_message(OnMessage on_message)
    {
        m_on_message = std::move(on_message);
    }

    // The opt-in metadata seam: a second callback that ALSO receives the per-message
    // message_info. When present it takes precedence over the 2-arg callback.
    template<typename OnMessageWithInfo>
    void on_message_with_info(OnMessageWithInfo cb)
    {
        m_on_message_with_info = std::move(cb);
    }

    // The node-shared receive route, threaded in by the registry so a reconnect slot REBUILD
    // carries it. The per-session seams take precedence; otherwise data flows through this.
    void on_message_route(plexus::detail::move_only_function<
                          void(std::string_view, std::span<const std::byte>, const message_info &)>
                                  cb)
    {
        m_on_message_route = std::move(cb);
    }

    // The node-shared object-lane route. READ-ONLY delivery — the carrier is valid for the
    // callback duration only; the deliver tail owns the single release on every path.
    void on_object_route(
            plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> cb)
    {
        m_on_object_route = std::move(cb);
    }

    // The transport-drop seam, fired from start()'s on_error wiring when an already-live
    // channel breaks. A clean tear_down does not fire it.
    void on_transport_drop(plexus::detail::move_only_function<void()> cb)
    {
        m_on_drop = std::move(cb);
    }

    void on_lifecycle(plexus::detail::move_only_function<void(const lifecycle_event &)> cb)
    {
        m_on_lifecycle = std::move(cb);
    }

    // The presence-stamp seam: the session routes a decoded heartbeat through it carrying THIS
    // peer's pinned node_id (absent = no stamp, so the session stays monitor-agnostic).
    void on_stamp_seen(plexus::detail::move_only_function<void(const node_id &)> cb)
    {
        m_on_stamp_seen = std::move(cb);
    }

    void on_security(plexus::detail::move_only_function<void(const security_event &)> cb)
    {
        m_on_security = std::move(cb);
    }

    void
    on_install_security(plexus::detail::move_only_function<void(const security_negotiation &)> cb)
    {
        m_negotiator.on_install_security(std::move(cb));
    }

    // Borrow the node-level security seam (OpenSSL-free transcript digest), owned by the build
    // context. Null (the default) is the no-AEAD/accept-any posture.
    void set_security_seam(const security_seam *seam) noexcept
    {
        m_negotiator.set_security_seam(seam);
    }

    void set_attach_entropy(security::rand_fn rand)
    {
        m_negotiator.set_attach_entropy(std::move(rand));
    }

    void set_attach_prover(security::attach_prover prover)
    {
        m_negotiator.set_attach_prover(std::move(prover));
    }

    // The authenticated peer's host identity, bound to the security step — never a
    // self-asserted wire claim. Absent until a security-engaged attach latches it.
    std::optional<node_id> authenticated_host_identity() const noexcept
    {
        return m_negotiator.authenticated_host_identity();
    }

    void on_subscribe_refused(
            plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> cb)
    {
        m_on_subscribe_refused = std::move(cb);
    }
    void
    on_subscribe_degraded(plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> cb)
    {
        m_on_subscribe_degraded = std::move(cb);
    }

    // The staleness gate runs BEFORE the router: a frame whose non-zero session_id differs
    // from the latched epoch is a previous-session straggler and is dropped; the first
    // non-zero observation latches the peer's epoch. A frame already posted before tear_down
    // is ignored (no phantom completion on a closed session).
    void on_receive(std::span<const std::byte> frame)
    {
        if(m_torn_down)
            return;
        auto hdr = wire::decode_header(frame);
        if(!hdr)
            return close_for_protocol_error(wire::close_cause::invalid_magic);
        if(hdr->session_id != 0 && m_peer_session_id != 0 && hdr->session_id != m_peer_session_id)
            return;
        if(hdr->session_id != 0 && m_peer_session_id == 0)
            m_peer_session_id = hdr->session_id;
        m_router.route(frame);
    }

    // The same-host receive companion's injection point: a framed message drained off the
    // co-host shared-memory ring enters the SAME receive path a wire frame does. The drain is
    // posted on the node executor, so this is never called inline from a wake. A torn-down
    // session ignores it (on_receive's guard).
    void inject_receive(std::span<const std::byte> frame) { on_receive(frame); }

    // Cancel the timer, detach the forwarders for this peer, reset the epoch latch and the
    // FSM for a fresh cycle, and close the channel. A teardown of a session that never
    // completed is not a disconnect — the prior-complete guard suppresses the spurious fire.
    void tear_down()
    {
        const bool was_complete = m_forwarders_installed;
        m_torn_down             = true;
        m_handshake_timer.cancel();
        m_messages.detach_all(m_msg_peer);
        m_procedures.detach_all(m_rpc_peer);
        m_peer_session_id          = 0;
        m_forwarders_installed     = false;
        m_outstanding_subscribes   = 0;
        m_ready_latched_this_cycle = false;
        m_fsm.on_torn_down();
        m_channel.close();
        if(was_complete)
            detail::fire_lifecycle(*this, lifecycle_edge::disconnected);
    }

    // The channel's protocol-close funnel. On a security-engaged session this close is a
    // stream-tamper teardown: the installed AEAD decorator routes a tag-verify failure through
    // here (a bad tag on an ordered stream is wire misbehavior with no honest resync), so fire
    // the dedicated event BEFORE the close. A plaintext session takes the plain close funnel.
    void on_channel_protocol_close(wire::close_cause cause)
    {
        if(m_torn_down)
            return;
        if(m_negotiator.engaged() && m_forwarders_installed)
            detail::fire_security(*this, security_kind::stream_tamper_teardown,
                                  security_cause::tag_verify_failed);
        close_for_protocol_error(cause);
    }

    // The uniform close funnel for a peer that misbehaved on the wire — BOTH the framing
    // violation surfaced by on_protocol_close and the semantic violations the session detects
    // on complete frames. It latches the protocol-error disposition so the on_error wiring
    // does NOT re-dial a peer we closed, emits exactly ONE warn, and tears down. Idempotent.
    void close_for_protocol_error(wire::close_cause)
    {
        if(m_torn_down)
            return;
        m_closed_for_protocol_error = true;
        m_logger.warn("plexus: peer_session protocol_close");
        tear_down();
    }

    // The counted subscribe emit: the session routes its OWN demand-subscribe so it observes
    // the wire emit and owns the readiness count. attach() returns true only on the 0->1
    // refcount transition — the single transition that puts a wire::subscribe on the channel.
    void subscribe(std::string_view fqn, const subscriber_qos &qos = subscriber_qos{},
                   std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(m_messages.attach(m_msg_peer, fqn, qos, type_id))
            ++m_outstanding_subscribes;
    }

    // The counted retire of one topic demand: detach through the forwarder's per-(peer, fqn)
    // gate, which emits the wire unsubscribe on its 1->0 transition.
    void unsubscribe(std::string_view fqn) { m_messages.detach(m_msg_peer, fqn); }

    // Emit a session-level keepalive heartbeat. Driven by the engine on the router tick
    // cadence — NOT a per-session timer. Reuses the session's send scratch.
    void emit_heartbeat()
    {
        wire::encode_heartbeat_into(m_payload_scratch, wire::heartbeat{});
        detail::send_control(*this, wire::msg_type::heartbeat);
    }

    // The readiness latch: once the outstanding count reaches 0 with the latch unset, fire
    // ready exactly once for this connection cycle. The latch and the counter both clear in
    // tear_down. A late subscribe after ready bumps the counter but does NOT re-arm the latch.
    void maybe_fire_ready()
    {
        if(m_outstanding_subscribes == 0 && !m_ready_latched_this_cycle)
        {
            m_ready_latched_this_cycle = true;
            detail::fire_lifecycle(*this, lifecycle_edge::ready);
        }
    }

    bool               is_complete() const noexcept { return m_forwarders_installed; }
    [[nodiscard]] bool same_host() const noexcept { return m_ctx.same_host; }
    std::uint64_t      session_id() const noexcept { return m_session_id; }
    std::uint64_t      peer_session_id() const noexcept { return m_peer_session_id; }
    const typename message_forwarder<Policy>::peer &msg_peer() const noexcept { return m_msg_peer; }
    const typename procedure_forwarder<Policy>::peer &rpc_peer() const noexcept
    {
        return m_rpc_peer;
    }

private:
    template<typename S>
    friend void detail::register_session_consumers(S &);
    template<typename S>
    friend void detail::deliver_session_data(S &, const wire::frame_header &,
                                             std::span<const std::byte>);
    template<typename S>
    friend void detail::deliver_session_object(S &, const object_carrier &);
    template<typename S>
    friend message_info detail::assemble_message_info(S &, const wire::frame_header &);
    template<typename S>
    friend void detail::execute(S &, const fsm_step_result &);
    template<typename S>
    friend void detail::on_abort(S &, handshake_outcome);
    template<typename S>
    friend void detail::on_complete(S &);
    template<typename S>
    friend void detail::resubscribe_all(S &);
    template<typename S>
    friend void detail::record_same_host(S &) noexcept;
    template<typename S>
    friend void detail::refuse_posture(S &);
    template<typename S>
    friend bool detail::channel_is_self_securing(const S &);
    template<typename S>
    friend void detail::fire_connect_edge(S &);
    template<typename S>
    friend peer_kind detail::session_kind(const S &) noexcept;
    template<typename S>
    friend void detail::fire_lifecycle(S &, lifecycle_edge, handshake_outcome);
    template<typename S>
    friend void detail::fire_security(S &, security_kind, security_cause);
    template<typename S>
    friend void detail::surface_subscribe_outcome(S &, const wire::subscribe_response &);
    template<typename S>
    friend void detail::on_subscribe_response_received(S &, std::span<const std::byte>);
    template<typename S>
    friend wire::handshake_request detail::self_request(const S &) noexcept;
    template<typename S>
    friend void detail::send_control(S &, wire::msg_type);
    template<typename S>
    friend void detail::send_handshake_request(S &);
    template<typename S>
    friend void detail::send_handshake_response(S &, handshake_outcome);

    void arm_handshake_timer()
    {
        m_handshake_timer.expires_after(
                std::chrono::duration_cast<std::chrono::milliseconds>(m_handshake_timeout));
        m_handshake_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(ec)
                        return; // cancelled by completion or teardown
                    detail::execute(*this, m_fsm.on_timeout());
                });
    }

    peer_context<Policy>        &m_ctx;
    channel_type                &m_channel;
    handshake_fsm_config         m_fsm_cfg;
    handshake_fsm                m_fsm;
    frame_router                 m_router;
    timer_type                   m_handshake_timer;
    message_forwarder<Policy>   &m_messages;
    procedure_forwarder<Policy> &m_procedures;
    std::chrono::nanoseconds     m_handshake_timeout;
    bool                         m_is_inbound_bootstrap;
    // A channel's delivery tier is fixed for the session's lifetime, so the intra-process
    // verdict is latched once here: deriving it per delivered message cost a getpeername
    // syscall plus endpoint formatting on every metadata-carrying delivery (~4% of the
    // receive-window cycles in the loopback TCP profile).
    bool          m_from_intra_process;
    std::uint64_t m_session_id{0}, m_peer_session_id{0};
    bool          m_forwarders_installed{false}, m_torn_down{false};
    // Latched true by close_for_protocol_error so the on_error wiring short-circuits the
    // re-dial: a peer WE closed for misbehavior must not be re-dialed.
    bool m_closed_for_protocol_error{false};
    // The readiness counter + fire-once latch, OWNED by the per-incarnation session (cleared
    // in tear_down so reconnect re-arms). The counter is the number of outstanding subscribe
    // acks; the latch records whether ready already fired this cycle.
    std::uint16_t                              m_outstanding_subscribes{0};
    bool                                       m_ready_latched_this_cycle{false};
    log::logger                               &m_logger;
    typename message_forwarder<Policy>::peer   m_msg_peer;
    typename procedure_forwarder<Policy>::peer m_rpc_peer;
    std::vector<std::byte>                     m_payload_scratch, m_frame_scratch;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>)>
            m_on_message;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>,
                                            const message_info &)>
            m_on_message_with_info;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>,
                                            const message_info &)>
            m_on_message_route;
    plexus::detail::move_only_function<void(std::string_view, const object_carrier &)>
                                                                      m_on_object_route;
    plexus::detail::move_only_function<void()>                        m_on_drop;
    plexus::detail::move_only_function<void(const lifecycle_event &)> m_on_lifecycle;
    plexus::detail::move_only_function<void(const node_id &)>         m_on_stamp_seen;
    plexus::detail::move_only_function<void(const security_event &)>  m_on_security;
    // The held-by-value security/transcript/proof orchestrator: owns the attach credentials +
    // pending attach, borrows the node-level seam, runs the PSK proof / transcript / posture
    // gate / AEAD install. The bridge forwards its security setters here.
    attach_negotiator<Policy> m_negotiator;
    plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)>
                                                                          m_on_subscribe_refused;
    plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> m_on_subscribe_degraded;
};

}

#endif

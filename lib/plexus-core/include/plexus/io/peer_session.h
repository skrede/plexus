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
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

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

// The per-peer bridge: one live connection turned into a session. It drives the
// pure handshake_fsm over an ALREADY-LIVE channel (the ctor performs no dial or
// listen so a future transport backend slots in unchanged), owns a per-peer
// frame_router whose consumers decode handshake frames into the FSM and hand data
// frames to the node-shared forwarders carrying THIS peer's identity, mints a
// per-connection session_id epoch on completion by drawing the next value from the
// per-peer record's epoch_source — the record OUTLIVES the incarnation (so a
// reconnect's fresh session is automatically a strictly-later epoch), and runs the receive-path
// staleness gate that latches the peer's first non-zero session_id and drops
// mismatches. Re-entrancy safety is structural: on_data is always posted and the
// lifetime is single-owner, so the bridge needs no synchronization wrapper and no
// inbound reassembler (the channel already delivers complete header-on frames).
template <typename Policy>
    requires plexus::Policy<Policy>
class peer_session
{
public:
    using channel_type = typename Policy::byte_channel_type;
    using executor_type = typename Policy::executor_type;
    using timer_type = typename Policy::timer_type;

    // The per-peer record bundles the per-incarnation DATA the session draws from:
    // ctx OWNS the live channel (the session borrows it), the peer node_name the
    // forwarder peers are keyed on, and the epoch well. The record OUTLIVES this
    // incarnation, so each successive session built against the same record mints a
    // strictly-later epoch. handshake_timeout stays required with NO default — a
    // session cannot arm a bound it was not told, and the value depends on the
    // deployment.
    peer_session(peer_context<Policy> &ctx, executor_type executor,
                 const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout,
                 message_forwarder<Policy> &messages, procedure_forwarder<Policy> &procedures,
                 bool is_inbound_bootstrap, log::logger &logger = shared_null_logger())
        : m_ctx(ctx), m_channel(*ctx.channel), m_fsm_cfg(fsm_cfg), m_fsm(fsm_cfg)
        , m_handshake_timer(executor), m_messages(messages), m_procedures(procedures)
        , m_handshake_timeout(handshake_timeout)
        , m_is_inbound_bootstrap(is_inbound_bootstrap), m_logger(logger)
        , m_msg_peer{m_channel, ctx.node_name}, m_rpc_peer{m_channel, ctx.node_name}
    {
    }

    // Wire the receive path, register the router consumers, arm the handshake
    // timer, and (for an outbound peer) begin the handshake. Two paired sessions
    // both begin outbound: the simultaneous-connect path completes on both ends.
    void start()
    {
        m_torn_down = false;
        m_closed_for_protocol_error = false;
        prime_self_credentials();
        m_channel.on_data([this](std::span<const std::byte> f) { on_receive(f); });
        m_channel.on_error([this](io_error) {
            if(m_on_drop && !m_torn_down && !m_closed_for_protocol_error)
                m_on_drop();
        });
        m_channel.on_protocol_close([this](wire::close_cause cause) { on_channel_protocol_close(cause); });
        register_consumers();
        arm_handshake_timer();
        if(!m_is_inbound_bootstrap)
        {
            execute(m_fsm.on_dial_started());
            execute(m_fsm.on_outbound_connected());
        }
    }

    template <typename OnMessage>
    void on_message(OnMessage on_message) { m_on_message = std::move(on_message); }

    // The opt-in metadata seam: a second callback that ALSO receives the per-message
    // message_info. Set ONCE at subscribe (the cold path), so the hot receive path
    // never allocates a callable. When present it takes precedence over the 2-arg
    // callback for delivered data; the 2-arg path is left entirely unchanged for a
    // subscriber that did not opt in.
    template <typename OnMessageWithInfo>
    void on_message_with_info(OnMessageWithInfo cb) { m_on_message_with_info = std::move(cb); }

    // The node-shared receive route, threaded in by the registry from the build
    // context at construction so a reconnect slot REBUILD carries it (unlike the
    // per-session on_message/on_message_with_info seams, which a rebuild loses). The
    // per-session seams take precedence when set; otherwise data flows through this.
    // Carries the message_info — a bytes-only consumer drops it. Absent = silent drop.
    void on_message_route(plexus::detail::move_only_function<
                          void(std::string_view, std::span<const std::byte>, const message_info &)> cb)
    {
        m_on_message_route = std::move(cb);
    }

    // The transport-drop seam, mirroring on_message: a plain settable callback
    // (absent = no routing) fired from start()'s on_error wiring when an
    // already-live channel breaks. The registry routes a dialed slot's drop to its
    // reconnect driver through this; a clean tear_down does not fire it.
    void on_transport_drop(plexus::detail::move_only_function<void()> cb) { m_on_drop = std::move(cb); }

    // The lifecycle seam, mirroring on_transport_drop: a settable callback the
    // registry wires to forward each edge up to the engine's posted fan-out. The
    // session never includes routing_engine.h — it routes edges blindly through this
    // seam (absent = no routing). Dormant until a fire-site calls fire_lifecycle.
    void on_lifecycle(plexus::detail::move_only_function<void(const lifecycle_event &)> cb) { m_on_lifecycle = std::move(cb); }

    // The presence-stamp seam, mirroring on_lifecycle: a settable callback the engine
    // wires to its liveliness monitor's stamp_seen. The session routes a decoded
    // heartbeat through it carrying THIS peer's pinned node_id (absent = no stamp, so
    // the session stays monitor-agnostic and includes no monitor header).
    void on_stamp_seen(plexus::detail::move_only_function<void(const node_id &)> cb) { m_on_stamp_seen = std::move(cb); }

    // The security-event seam, mirroring on_lifecycle: the registry wires it to forward
    // each dedicated security event (unauthorized attach, downgrade/posture refusal,
    // stream-tamper teardown) up to the engine's posted fan-out (absent = no routing).
    void on_security(plexus::detail::move_only_function<void(const security_event &)> cb) { m_on_security = std::move(cb); }

    // The per-session AEAD-install hook, set by the registry from the node-level seam
    // plus THIS session's just-built channel. It is invoked exactly once on a successful
    // security-engaged attach over a plaintext network channel: the gated layer derives
    // the keys from the negotiation and installs the decorator over the captured channel.
    // Absent = no AEAD posture (the accept-any plaintext path is unchanged). It stays
    // OpenSSL-free here — the session only stores and calls a type-erased callable.
    void on_install_security(plexus::detail::move_only_function<void(const security_negotiation &)> cb)
    {
        m_install_security = std::move(cb);
    }

    // Borrow the node-level security seam (the OpenSSL-free transcript digest), owned by
    // the build context — ONE per node. Set once by the registry before start();
    // null (the default) is the no-AEAD/accept-any posture. Borrowed, never owned: the
    // move-only seam is shared by every session without a per-session copy.
    void set_security_seam(const security_seam *seam) noexcept { m_install_security_seam = seam; }

    // The per-session entropy seam: the deployment's CSPRNG, used ONCE at start() to mint
    // this session's own_nonce so the key schedule salt varies per session (no all-zero
    // nonce, no cross-session keystream reuse). Absent on the accept-any plaintext path —
    // own_nonce then stays zero and the unused proof field rides along harmlessly.
    void set_attach_entropy(security::rand_fn rand) { m_rand = std::move(rand); }

    // The attaching-side proof seam: this node's OWN keyed PSK plus the SAME hmac_fn the
    // verifier holds. The key_id is stamped onto the outbound request/response, and the
    // responder MACs the dialer's facts-view into the wire proof field. Absent = no proof
    // stamped (the accept-any path). Set once at session build, mirroring set_security_seam.
    void set_attach_prover(security::attach_prover prover) { m_prover = std::move(prover); }

    // The authenticated peer's host identity, bound to the security step (SPKI-derived
    // for a TLS peer, attach-bound for a PSK peer) — never a self-asserted TXT/wire
    // claim. Absent until the attach resolves; a security-engaged attach latches it from
    // the verified facts at completion, so a spoofed external identity claim is ignored.
    std::optional<node_id> authenticated_host_identity() const noexcept { return m_authenticated_host_identity; }

    // The subscribe-outcome seams, mirroring on_lifecycle: an arriving subscribe_response
    // that REFUSES a match (type_mismatch / incompatible_qos / source_identity_incompatible)
    // fires on_subscribe_refused; a permissive degraded-accept (subscribed_degraded) fires
    // on_subscribe_degraded carrying the surfaced unsatisfied-field bitmask — the
    // non-silent guarantee. Absent = no surfacing (the readiness decrement still runs).
    void on_subscribe_refused(plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> cb)
    {
        m_on_subscribe_refused = std::move(cb);
    }
    void on_subscribe_degraded(plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> cb)
    {
        m_on_subscribe_degraded = std::move(cb);
    }

    // The staleness gate runs BEFORE the router: a frame whose non-zero session_id
    // differs from the latched epoch is a previous-session straggler and is dropped;
    // the first non-zero observation latches the peer's epoch. A frame already posted
    // before tear_down is ignored (no phantom completion on a closed session).
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

    // Cancel the timer, detach the forwarders for this peer, reset the epoch latch
    // and the FSM for a fresh cycle, and close the channel.
    void tear_down()
    {
        const bool was_complete = m_forwarders_installed;
        m_torn_down = true;
        m_handshake_timer.cancel();
        m_messages.detach_all(m_msg_peer);
        m_procedures.detach_all(m_rpc_peer);
        m_peer_session_id = 0;
        m_forwarders_installed = false;
        m_outstanding_subscribes = 0;
        m_ready_latched_this_cycle = false;
        m_fsm.on_torn_down();
        m_channel.close();
        // A teardown of a session that never completed is not a disconnect — the
        // prior-complete guard suppresses the spurious fire (an un-established abort
        // fires rejected only).
        if(was_complete)
            fire_lifecycle(lifecycle_edge::disconnected);
    }

    // The channel's protocol-close funnel. On a security-engaged session this close is a
    // stream-tamper teardown: the installed AEAD decorator routes a tag-verify failure
    // through on_protocol_close (a bad tag on an ordered stream is wire misbehavior with
    // no honest resync), so fire the dedicated stream_tamper_teardown event with
    // its cause BEFORE the close. A plaintext (un-engaged) session takes the plain close
    // funnel unchanged. Datagram drops never reach here — the datagram decorator counts
    // them and never tears down.
    void on_channel_protocol_close(wire::close_cause cause)
    {
        if(m_torn_down)
            return;
        if(m_pending_attach.engaged && m_forwarders_installed)
            fire_security(security_kind::stream_tamper_teardown, security_cause::tag_verify_failed);
        close_for_protocol_error(cause);
    }

    // The uniform close funnel for a peer that misbehaved on the wire — BOTH the
    // framing violation surfaced by the channel's on_protocol_close and the semantic
    // violations the session detects on complete frames (a header that does not
    // decode, an FSM abort). It latches the protocol-error disposition so the
    // on_error wiring does NOT re-dial a peer we closed, emits exactly ONE warn
    // (never per-byte), and tears down. Idempotent on m_torn_down, mirroring
    // on_receive's guard.
    void close_for_protocol_error(wire::close_cause)
    {
        if(m_torn_down)
            return;
        m_closed_for_protocol_error = true;
        m_logger.warn("plexus: peer_session protocol_close");
        tear_down();
    }

    // The counted subscribe emit: the session routes its OWN demand-subscribe so it
    // observes the wire emit and owns the readiness count. attach() returns true only
    // on the 0->1 refcount transition — the single transition that puts a
    // wire::subscribe on the channel — so the counter tracks exactly the outstanding
    // acks. The forwarder stays readiness-agnostic: it is told to attach (a byte fact)
    // and learns nothing of the count.
    void subscribe(std::string_view fqn, const subscriber_qos &qos = subscriber_qos{})
    {
        if(m_messages.attach(m_msg_peer, fqn, qos))
            ++m_outstanding_subscribes;
    }

    // Emit a session-level keepalive heartbeat: encode the fixed-width presence
    // payload and frame it as msg_type::heartbeat onto this peer's channel (session_id
    // 0, consistent with every control frame). Driven by the engine on the router tick
    // cadence — NOT a per-session timer. Reuses the session's send scratch.
    void emit_heartbeat()
    {
        wire::encode_heartbeat_into(m_payload_scratch, wire::heartbeat{});
        send_control(wire::msg_type::heartbeat);
    }

    bool is_complete() const noexcept { return m_forwarders_installed; }
    std::uint64_t session_id() const noexcept { return m_session_id; }
    std::uint64_t peer_session_id() const noexcept { return m_peer_session_id; }
    const typename message_forwarder<Policy>::peer &msg_peer() const noexcept { return m_msg_peer; }
    const typename procedure_forwarder<Policy>::peer &rpc_peer() const noexcept { return m_rpc_peer; }

private:
    void register_consumers()
    {
        m_router.on_handshake_req([this](std::span<const std::byte> inner) {
            if(auto req = wire::decode_handshake_request(inner))
            {
                // Inbound request: the local node is the RESPONDER (it verifies the
                // dialer's attach proof). Assemble the facts from the decoded peer region.
                m_pending_attach = assemble_attach_facts(*req, security::attach_role::responder, req->id);
                latch_peer_proof(*req);
                if(posture_mismatched(*req))
                    return refuse_posture();
                execute(m_fsm.on_request(*req, m_is_inbound_bootstrap, m_pending_attach.facts));
            }
        });
        m_router.on_handshake_resp([this](std::span<const std::byte> inner) {
            if(auto resp = wire::decode_handshake_response(inner))
            {
                // Inbound response: the local node is the INITIATOR (it dialed out).
                m_pending_attach = assemble_attach_facts(*resp, security::attach_role::initiator, resp->id);
                latch_peer_proof(*resp);
                if(posture_mismatched(*resp))
                    return refuse_posture();
                execute(m_fsm.on_response(*resp, m_pending_attach.facts));
            }
        });
        m_router.on_subscribe([this](std::span<const std::byte> inner) {
            if(auto req = wire::decode_subscribe_request(inner))
            {
                // type_hash == 0 is the undeclared-type sentinel on the wire; lift it
                // to std::nullopt so an undeclared subscriber is never refused.
                std::optional<std::uint64_t> subscriber_type_id;
                if(req->type_hash != 0)
                    subscriber_type_id = req->type_hash;
                // Lift the carried QoS region into the core choice; an absent region
                // (has_qos clear) lands as the friendly default.
                const subscriber_qos sub_qos = req->has_qos ? from_wire_region(req->qos)
                                                            : subscriber_qos{};
                m_messages.attach_for_fanout(m_msg_peer, req->fqn, subscriber_type_id, sub_qos);
            }
        });
        m_router.on_fetch_latched([this](std::span<const std::byte> inner) {
            // The consumer-paced PULL: decode the request and replay the capped
            // retained window to THIS requesting peer. The reply is the data frames
            // themselves, so no reply control frame is sent.
            if(auto req = wire::decode_fetch_latched_request(inner))
                m_messages.fetch_latched(m_msg_peer, req->topic_hash, req->max_samples);
        });
        m_router.on_subscribe_response([this](std::span<const std::byte> inner) {
            on_subscribe_response_received(inner);
        });
        m_router.on_unidirectional([this](const wire::frame_header &hdr, std::span<const std::byte> inner) {
            deliver_data(hdr, inner);
        });
        m_router.on_rpc_request([this](std::span<const std::byte> inner) {
            m_procedures.deliver_request(m_rpc_peer, inner, m_session_id);
        });
        m_router.on_rpc_response([this](std::span<const std::byte> inner) {
            m_procedures.deliver_response(m_rpc_peer, inner);
        });
        // A heartbeat is a session-level presence assert: decode it (bounds-safe,
        // untrusted input) and stamp THIS pinned peer's last-seen ONLY — presence,
        // never a topic deadline. No periodic timer is armed here; the ONE ticker is
        // the engine's monitor.
        m_router.on_heartbeat([this](std::span<const std::byte> inner) {
            if(wire::decode_heartbeat(inner) && m_on_stamp_seen)
                m_on_stamp_seen(m_ctx.peer_id);
        });
    }

    // The message_info assembly seam: this is the ONLY place the decoded frame_header is
    // still live alongside the data, because the router strips it before deliver. Stamp
    // the header-derived metadata HERE — source_timestamp is the publisher's wire
    // timestamp; reception_timestamp is receiver-stamped from the same clock the codec
    // uses (so it is monotonic with respect to the publisher's stamp); from_intra_process
    // is derived honestly from THIS channel's own endpoint scheme (true only on a genuine
    // same-process inproc delivery, never from peer-supplied data). publication_sequence
    // and source_identity are filled by the forwarder, which owns the inner-payload decode.
    //
    // has_source_identity is read from the gid flag and passed to BOTH deliver paths: the
    // producer emits the varint counter per ITS topic declaration, so even the bytes-only
    // 2-arg path must honor the flag to land the data span correctly. The gid's node_id
    // half is m_ctx.peer_id — the PINNED session peer (the direct-delivery invariant) —
    // never a node_id taken from the frame. When the opt-in 3-arg callback is set it takes
    // precedence; otherwise the unchanged 2-arg path runs.
    void deliver_data(const wire::frame_header &hdr, std::span<const std::byte> inner)
    {
        const bool has_source_identity = (hdr.flags & wire::k_flag_source_identity) != 0;
        // Precedence: a per-session info/bytes seam (the test/cold-path install) wins
        // when set; otherwise the node-shared route (threaded by the registry, so it
        // survives a reconnect rebuild) delivers. The shared route carries the
        // message_info, so its delivery rides the same info-assembling path. None set
        // = silent drop.
        if(m_on_message_with_info)
        {
            const message_info info = assemble_message_info(hdr);
            m_messages.deliver(m_msg_peer, inner, info, m_ctx.peer_id, has_source_identity,
                               [this](std::string_view fqn, std::span<const std::byte> data,
                                      const message_info &mi) {
                                   m_on_message_with_info(fqn, data, mi);
                               });
            return;
        }
        if(m_on_message)
        {
            m_messages.deliver(m_msg_peer, inner, m_ctx.peer_id, has_source_identity,
                               [this](std::string_view fqn, std::span<const std::byte> data) {
                                   m_on_message(fqn, data);
                               });
            return;
        }
        if(m_on_message_route)
        {
            const message_info info = assemble_message_info(hdr);
            m_messages.deliver(m_msg_peer, inner, info, m_ctx.peer_id, has_source_identity,
                               [this](std::string_view fqn, std::span<const std::byte> data,
                                      const message_info &mi) {
                                   m_on_message_route(fqn, data, mi);
                               });
            return;
        }
        m_messages.deliver(m_msg_peer, inner, m_ctx.peer_id, has_source_identity,
                           [](std::string_view, std::span<const std::byte>) {});
    }

    message_info assemble_message_info(const wire::frame_header &hdr)
    {
        message_info info{};
        info.source_timestamp    = hdr.timestamp_ns;
        info.reception_timestamp = wire::now_timestamp_ns();
        info.from_intra_process  = tier_of(m_channel.remote_endpoint().scheme) == locality::process;
        return info;
    }

    // Map an FSM action to channel I/O. retry stays the FSM's intent as a log line:
    // the reconnect driver lives outside the session and observes the transport's
    // dial-failure directly, so the session is not reshaped to own a dialer.
    void execute(const fsm_step_result &step)
    {
        switch(step.action)
        {
            case fsm_action::send_request:  return send_handshake_request();
            case fsm_action::send_response: return send_handshake_response(step.outcome);
            case fsm_action::complete:      return on_complete();
            case fsm_action::retry:         return m_logger.warn("plexus: peer_session retry_no_dialer");
            case fsm_action::abort:         return on_abort(step.outcome);
            case fsm_action::none:          return;
        }
    }

    // An FSM abort: fire rejected carrying the real refusal reason (reject_version /
    // reject_identity) BEFORE the close. The abort path is an un-established session
    // (m_forwarders_installed is false), so close_for_protocol_error's tear_down
    // suppresses disconnected via its prior-complete guard — only rejected fires.
    void on_abort(handshake_outcome reason)
    {
        fire_lifecycle(lifecycle_edge::rejected, reason);
        if(reason == handshake_outcome::reject_unauthorized)
            fire_security(classify_unauthorized());
        close_for_protocol_error(wire::close_cause::invalid_magic);
    }

    // Distinguish WHY the attach gate refused so the security event carries the cause an
    // observer acts on. A posture mismatch is caught earlier (posture_mismatched ->
    // refuse_posture), so a gate refusal on a SECURED pair is a transcript-bound proof
    // failure — a forced downgrade (a stripped/forced cipher offer changed the digest the
    // proof covered). A refusal with no security posture is a plain unauthorized attach.
    security_kind classify_unauthorized() const noexcept
    {
        return m_pending_attach.engaged ? security_kind::downgrade_refused
                                        : security_kind::unauthorized_attach;
    }

    bool seam_engaged() const noexcept
    {
        return m_install_security_seam != nullptr && m_install_security_seam->engaged();
    }

    // The bridge-assembled attach context: the facts the gate decides on plus the
    // negotiation the AEAD key schedule needs. Both are filled from the SAME decoded
    // wire region so the digest the gate bound is the digest the keys derive from.
    struct pending_attach
    {
        security::attach_facts facts{};
        security_negotiation   negotiation{};
        bool                   engaged{false};
    };

    // Assemble the attach facts + negotiation from a decoded handshake frame (request or
    // response — both carry the identical attach region). The peer's own_nonce is THIS
    // side's verifier challenge in reverse: the peer chose it, so from the local node's
    // standpoint it is the peer_nonce the proof must cover (anti-reflection). The
    // transcript digest folds the negotiation through the injected (OpenSSL-free) seam;
    // a disengaged seam leaves the digest zeroed and the accept-any path unchanged. The
    // initiator/responder ids are pinned by role: the local node is one, the peer the
    // other. No crypto runs here — the proof recompute lives in the policy, the key
    // derivation behind the install hook.
    template <typename Frame>
    pending_attach assemble_attach_facts(const Frame &frame, security::attach_role local_role,
                                         const node_id &peer_id) const
    {
        pending_attach out;
        out.engaged = m_fsm.attach_policy() != nullptr && seam_engaged();
        out.negotiation.key_id          = frame.key_id;
        out.negotiation.role            = local_role;
        out.negotiation.chosen_cipher   = frame.chosen_cipher;
        // The local node's own challenge rides self_request().own_nonce; the peer's rides
        // the decoded frame. initiator/responder nonces are assigned by the local role.
        const auto own_nonce = self_request().own_nonce;
        out.negotiation.initiator_nonce = local_role == security::attach_role::initiator ? own_nonce : frame.own_nonce;
        out.negotiation.responder_nonce = local_role == security::attach_role::initiator ? frame.own_nonce : own_nonce;
        out.facts.key_id            = frame.key_id;
        out.facts.initiator_id      = local_role == security::attach_role::initiator ? m_fsm_cfg.self_id : peer_id;
        out.facts.responder_id      = local_role == security::attach_role::initiator ? peer_id : m_fsm_cfg.self_id;
        out.facts.peer_nonce        = frame.own_nonce;
        out.facts.own_nonce         = own_nonce;
        out.facts.role              = local_role;
        if(out.engaged)
            compute_transcript(out, frame);
        out.negotiation.transcript_digest = out.facts.transcript_digest;
        return out;
    }

    // Latch the decoded wire proof into a stable member buffer and point facts.proof at
    // it: the policy's ct_equal reads the span THROUGH the gate's decide() call, and a span
    // into the transient decoded frame would dangle. The buffer is the session's, so it
    // outlives the synchronous gate. Called from the receive handlers AFTER the facts are
    // assembled, before the FSM gate runs.
    template <typename Frame>
    void latch_peer_proof(const Frame &frame)
    {
        m_peer_proof = frame.proof;
        m_pending_attach.facts.proof = m_peer_proof;
    }

    // The posture gate: a secured node (an engaged seam) and a plain peer (no
    // cipher offered) are a hard mismatch BOTH ways — a secured local meeting a plain
    // offer, OR a plain local meeting a secured offer — refused fail-closed with no
    // silent plaintext fallback. The peer's posture is read from cipher_offer (a non-zero
    // offer means it proposes AEAD). This runs ahead of the FSM accept so a mismatched
    // pair never resolves.
    template <typename Frame>
    bool posture_mismatched(const Frame &frame) const noexcept
    {
        const bool local_secured = seam_engaged();
        const bool peer_secured = frame.cipher_offer != 0;
        return local_secured != peer_secured;
    }

    // Fold the negotiation transcript (cipher_offer ‖ chosen ‖ protocol_version ‖ both
    // nonces) into the facts' digest via the seam's injected hash. The transcript bytes
    // are assembled in a fixed order both ends agree on; a stripped/forced offer changes
    // the digest, so the gate's recomputed proof — and the derived keys — differ
    // (downgrade refusal).
    template <typename Frame>
    void compute_transcript(pending_attach &out, const Frame &frame) const
    {
        std::array<std::byte, 1 + 1 + 1 + 16 + 16> transcript{};
        std::size_t n = 0;
        transcript[n++] = static_cast<std::byte>(frame.cipher_offer);
        transcript[n++] = static_cast<std::byte>(frame.chosen_cipher);
        transcript[n++] = static_cast<std::byte>(wire::k_protocol_version);
        for(auto b : out.negotiation.initiator_nonce) transcript[n++] = b;
        for(auto b : out.negotiation.responder_nonce) transcript[n++] = b;
        std::array<std::byte, 32> digest{};
        if(m_install_security_seam->compute(transcript, digest))
            out.facts.transcript_digest = digest;
    }

    // The response reuses every request field and adds a status, so this is the one
    // place the self identity/versions are stamped onto the wire. A secured node (an
    // engaged seam) advertises its AEAD posture in the cipher offer/chosen so the peer's
    // posture gate sees a secured-vs-secured pair; a plain node leaves them zero. This is
    // what makes the posture mismatch observable both ways.
    // Mint this session's own_nonce ONCE (a fresh per-session salt for the key schedule)
    // and adopt the prover's key_id when an attach prover is engaged. Idempotent: a second
    // start() (a reconnect cycle) re-mints a fresh nonce, but a single cycle fills exactly
    // once so the SAME value feeds self_request, the transcript, and the proof. A degraded
    // RNG (rand_fn returns false) leaves the nonce zeroed on a best-effort basis — the
    // engaged crypto path additionally depends on the seam being installed, which the
    // deployment owns. The accept-any path (no rand_fn) leaves the nonce zero unchanged.
    void prime_self_credentials() noexcept
    {
        if(m_rand)
            (void)m_rand(m_own_nonce);
        if(m_prover.engaged())
            m_key_id = m_prover.key_id();
    }

    wire::handshake_request self_request() const noexcept
    {
        const std::uint8_t offer = seam_engaged() ? wire::cipher_offer_bits::chacha20_poly1305 : 0;
        return {.id                       = m_fsm_cfg.self_id,
                .version_major            = m_fsm_cfg.version_major,
                .version_minor            = m_fsm_cfg.version_minor,
                .compatible_version_major = m_fsm_cfg.compatible_version_major,
                .compatible_version_minor = m_fsm_cfg.compatible_version_minor,
                .protocol_version         = wire::k_protocol_version,
                .fingerprint              = m_fsm_cfg.local_fingerprint.value,
                .key_id                   = m_key_id,
                .own_nonce                = m_own_nonce,
                .cipher_offer             = offer,
                .chosen_cipher            = offer,
                .proof                    = {}};
    }

    void send_handshake_request()
    {
        wire::encode_handshake_request_into(m_payload_scratch, self_request());
        send_control(wire::msg_type::handshake_req);
    }

    // The response is the one leg the local node proves on: it is the responder for an
    // inbound request, so it stamps the transcript-bound, reflection-resistant proof the
    // dialer verifies on its on_response. The proof MACs the DIALER's facts-view (the
    // dialer's role, its own_nonce as peer_nonce, the both-nonce transcript) so the
    // dialer — recomputing under the shared PSK with its mirror view — matches byte for
    // byte. A request carries no proof: a 1-RTT PSK dialer cannot prove before it has
    // seen the responder's nonce, so the request leg stays accept-any (a known 1-RTT
    // limitation). The proof is stamped only when the prover is engaged and the pending
    // attach assembled a transcript-bound facts view for this peer.
    void send_handshake_response(handshake_outcome outcome)
    {
        const auto r = self_request();
        wire::handshake_response resp{.id = r.id, .version_major = r.version_major,
                .version_minor = r.version_minor, .compatible_version_major = r.compatible_version_major,
                .compatible_version_minor = r.compatible_version_minor,
                .protocol_version = r.protocol_version, .fingerprint = r.fingerprint,
                .key_id = r.key_id, .own_nonce = r.own_nonce, .cipher_offer = r.cipher_offer,
                .chosen_cipher = r.chosen_cipher, .proof = stamped_response_proof(),
                .status = status_for(outcome)};
        wire::encode_handshake_response_into(m_payload_scratch, resp);
        send_control(wire::msg_type::handshake_resp);
    }

    // Compute the proof the dialer verifies: the responder reconstructs the DIALER's
    // attach_facts view (the dialer is the initiator, so its own_nonce is the responder's
    // peer_nonce and vice-versa) and MACs the canonical attach_proof_input under the shared
    // PSK. The pending attach (assembled on this side's on_request) already carries the
    // role-mirrored ids/nonces/transcript for the peer, so the dialer-view facts are that
    // value with the role and nonces swapped to the dialer's standpoint. A disengaged
    // prover returns a zero proof (the accept-any path leaves the field unused).
    std::array<std::byte, wire::k_handshake_proof_len> stamped_response_proof() const
    {
        std::array<std::byte, wire::k_handshake_proof_len> proof{};
        if(!m_prover.engaged())
            return proof;
        security::attach_facts dialer_view = m_pending_attach.facts;
        dialer_view.role       = security::attach_role::initiator;
        std::swap(dialer_view.peer_nonce, dialer_view.own_nonce);
        (void)m_prover.prove(dialer_view, proof);
        return proof;
    }

    static wire::handshake_status status_for(handshake_outcome outcome) noexcept
    {
        switch(outcome)
        {
            case handshake_outcome::reject_version:      return wire::handshake_status::version_incompatible;
            case handshake_outcome::reject_identity:     return wire::handshake_status::identity_conflict;
            case handshake_outcome::reject_unauthorized: return wire::handshake_status::unauthorized;
            default:                                     return wire::handshake_status::accepted;
        }
    }

    // Handshake control always carries session_id 0 (it is what mints the epoch).
    void send_control(wire::msg_type type)
    {
        wire::frame_header fhdr{
                .type         = type,
                .flags        = 0,
                .session_id   = 0,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = m_payload_scratch.size()
        };
        wire::encode_frame_into(m_frame_scratch, fhdr, m_payload_scratch);
        m_channel.send(m_frame_scratch);
    }

    // Install-once: a second complete (the simultaneous-connect tail) no-ops. The
    // dedup verdict's keep_outbound/keep_inbound channel drop is a registry-block
    // concern — single-peer here, so it is read but not acted on.
    //
    // A bootstrap inbound completes with no counter-dial in flight (a peer dialed us
    // before we had demand to dial back — the common path under demand-driven lazy
    // dial), so it must still send the accept response: that is how the dialer learns
    // it was accepted, completes, and mints its own epoch. The simultaneous-connect
    // path (both ends dialed, is_inbound_bootstrap false) completes off the mutual
    // requests and must send no redundant response — the same flag distinguishes them.
    void on_complete()
    {
        if(m_forwarders_installed)
            return;
        if(m_is_inbound_bootstrap)
            send_handshake_response(handshake_outcome::accept_inbound);
        m_handshake_timer.cancel();
        mint_session_id();
        record_same_host();
        if(!install_security_on_accept())
            return refuse_posture();
        m_forwarders_installed = true;
        const bool reconnected = m_ctx.has_ever_connected;
        fire_connect_edge();
        // Reconnect resurrection: re-emit the remembered subscribes through the counted
        // path BEFORE the readiness check, so the resurrected acks are outstanding and
        // ready cannot fire until they return. On a first connect there is nothing
        // remembered, so this is a no-op and the zero-subscribe path below fires ready
        // immediately.
        // Resurrect the durable subscribe demand on EVERY completion — both the first
        // connect (the demand a lazy subscribe-that-triggered-the-dial recorded before
        // the session existed) and a reconnect — through the counted path, so the
        // demanded subscribes are outstanding when ready is evaluated. The forwarder is
        // the durable demand ledger; (void)reconnected because the resurrection set is
        // the same either way (empty for a true zero-subscribe peer, which fires ready
        // immediately). This is the first-publish-loss guard: a subscribe issued before
        // the connection completed is never lost.
        (void)reconnected;
        resubscribe_all();
        maybe_fire_ready();
    }

    // Re-emit every remembered subscribe for this peer through the counted subscribe(),
    // so each 0->1 wire-emit increments the outstanding count. The remembered topics
    // are read from the node-shared forwarder by a pure-read accessor (the forwarder
    // stays readiness-agnostic). The demand survives a teardown — only a genuine
    // unsubscribe forgets it — and the engine records it the moment subscribe is called
    // (even before the session exists), so this resurrects both a lazy first-connect
    // subscribe and a reconnect's still-demanded set.
    void resubscribe_all()
    {
        for(const auto &demand : m_messages.remembered_topics(m_ctx.node_name))
            subscribe(demand.fqn, demand.qos);
    }

    void mint_session_id() noexcept { m_session_id = m_ctx.epochs.next(); }

    // The same-host eligibility verdict: compare the peer's advertised
    // fingerprint (learned by the FSM at the validated request/response) to our own
    // via is_same_host — a null/absent peer fingerprint is conservatively NOT
    // same-host (the fail-closed null-guard) — and record it on the per-peer record.
    // This is the ELIGIBILITY gate the shared-memory upgrade reads: a pair that is
    // not same-host never attempts a ring acquire, regardless of any dispatch hint.
    void record_same_host() noexcept
    {
        m_ctx.same_host =
            shm::is_same_host(m_fsm.last_seen_peer_fingerprint(), m_fsm.local_fingerprint());
    }

    // On a security-engaged accept, latch the authenticated host identity from the
    // VERIFIED facts (never a wire claim) and install the AEAD decorator over a plaintext
    // network channel. A transport that already authenticates-encrypts (a TLS/DTLS
    // channel) is NOT double-wrapped — the attach still bound the identity, but no AEAD
    // decorator is installed. The install hook runs before the forwarders so subsequent
    // frames are sealed; the bridge holds the erased channel and the gated layer owns the
    // EVP instantiation.
    // Returns false on a fail-closed posture refusal: a security-engaged accept over ANY
    // plaintext network channel (stream OR datagram) with no AEAD install hook is refused,
    // never silently proceeded — without the decorator a secured posture would transmit
    // cleartext while reporting a latched authenticated identity. The two transports fail
    // the same way; there is no stream-only fail-open exception.
    bool install_security_on_accept()
    {
        if(!m_pending_attach.engaged)
            return true;
        m_authenticated_host_identity = authenticated_peer_id(m_pending_attach.facts);
        if(channel_is_self_securing())
            return true;
        if(!m_install_security)
            return false;
        m_install_security(m_pending_attach.negotiation);
        return true;
    }

    // The fail-closed posture refusal: clear the latched identity, fire the posture
    // security event + the lifecycle rejected edge, and tear down — no silent plaintext
    // fallback. Mirrors on_abort's edge ordering for an un-established session.
    void refuse_posture()
    {
        m_authenticated_host_identity.reset();
        fire_lifecycle(lifecycle_edge::rejected, handshake_outcome::reject_unauthorized);
        fire_security(security_kind::posture_mismatch);
        close_for_protocol_error(wire::close_cause::invalid_magic);
    }

    // A transport that already provides authenticated encryption: its scheme names a
    // crypto handshake. The AEAD decorator decorates ONLY plaintext network channels, so
    // a self-securing channel is left undecorated (no double-wrap).
    bool channel_is_self_securing() const
    {
        const auto scheme = m_channel.remote_endpoint().scheme;
        return scheme == "tls" || scheme == "dtls";
    }

    // Fire connected on the first-ever complete, reconnected on every subsequent
    // one. The discriminator is the long-lived has_ever_connected flag on the
    // record (it survives teardown): read its PRIOR value, then latch it true so it
    // never clears. The read MUST precede the set or every complete looks like a
    // reconnect.
    void fire_connect_edge()
    {
        const bool first = !m_ctx.has_ever_connected;
        m_ctx.has_ever_connected = true;
        fire_lifecycle(first ? lifecycle_edge::connected : lifecycle_edge::reconnected);
    }

    // The peer_kind discriminator drawn from the one source of truth: an inbound
    // bootstrap is an accepted peer, an outbound dial a dialed peer.
    peer_kind kind() const noexcept
    {
        return m_is_inbound_bootstrap ? peer_kind::accepted : peer_kind::dialed;
    }

    // Route one lifecycle edge up the seam (if wired) as an owned-value carrier. No
    // fire-site calls this yet — the seam is dormant in this increment; the edges
    // land in a later increment.
    void fire_lifecycle(lifecycle_edge edge, handshake_outcome reason = handshake_outcome::none)
    {
        if(!m_on_lifecycle)
            return;
        m_on_lifecycle(lifecycle_event{edge, m_ctx.peer_id, m_ctx.node_name, kind(), reason});
    }

    // Route one security event up the dedicated seam (if wired) carrying THIS peer's
    // pinned id. The cause is meaningful only on the stream-tamper kind. Datagram
    // replay/tamper drops never reach here — they are counted on the decorator and read
    // on demand (the observer must not become the DoS).
    void fire_security(security_kind kind, security_cause cause = security_cause::none)
    {
        if(!m_on_security)
            return;
        m_on_security(security_event{kind, m_ctx.peer_id, cause});
    }

    // The consumer half of the subscribe loop: an arriving subscribe_response drives
    // the readiness decrement. A malformed frame is dropped without touching the
    // counter. The underflow guard is input validation on an unauthenticated transport:
    // a response with no outstanding match is warned-and-dropped BEFORE the decrement,
    // so a stray or duplicate ack can never wrap the uint16_t (which would make ready
    // unreachable for the cycle).
    void on_subscribe_response_received(std::span<const std::byte> inner)
    {
        auto resp = wire::decode_subscribe_response(inner);
        if(!resp)
            return;
        // The anti-wrap guard stays FIRST: a response with no outstanding match is a
        // stray/duplicate/forged frame and is warned-and-dropped BEFORE anything else,
        // so it can never fire a refusal/degraded callback (nor wrap the counter).
        if(m_outstanding_subscribes == 0)
            return m_logger.warn("plexus: subscribe_response with no outstanding match");
        // Surface the match outcome to the subscriber (the response was previously
        // silently swallowed). A refusal fires on_subscribe_refused; a permissive
        // degraded-accept fires on_subscribe_degraded with the unsatisfied-field set.
        surface_subscribe_outcome(*resp);
        --m_outstanding_subscribes;
        maybe_fire_ready();
    }

    // Fire the subscribe-outcome observables for a matched response. Kept separate so
    // the handler reads as guard -> surface -> decrement.
    void surface_subscribe_outcome(const wire::subscribe_response &resp)
    {
        if(is_refusal(resp.status))
        {
            if(m_on_subscribe_refused)
                m_on_subscribe_refused(resp.topic_hash, resp.status);
        }
        else if(resp.status == wire::subscribe_status::subscribed_degraded)
        {
            if(m_on_subscribe_degraded)
                m_on_subscribe_degraded(resp.topic_hash, resp.degraded_flags);
        }
    }

    static bool is_refusal(wire::subscribe_status s)
    {
        return s == wire::subscribe_status::type_mismatch
            || s == wire::subscribe_status::incompatible_qos
            || s == wire::subscribe_status::source_identity_incompatible;
    }

    // The readiness latch: once the outstanding count reaches 0 with the latch unset,
    // fire ready exactly once for this connection cycle. The latch and the counter
    // both clear in tear_down, so the next incarnation re-arms. A late subscribe issued
    // after ready fires bumps the counter but does NOT re-arm the latch, so it cannot
    // fire a second ready this cycle. Driven from on_complete's tail (the zero-subscribe
    // case) and from the subscribe_response decrement.
    void maybe_fire_ready()
    {
        if(m_outstanding_subscribes == 0 && !m_ready_latched_this_cycle)
        {
            m_ready_latched_this_cycle = true;
            fire_lifecycle(lifecycle_edge::ready);
        }
    }

    void arm_handshake_timer()
    {
        m_handshake_timer.expires_after(
                std::chrono::duration_cast<std::chrono::milliseconds>(m_handshake_timeout));
        m_handshake_timer.async_wait([this](std::error_code ec) {
            if(ec)
                return;   // cancelled by completion or teardown
            execute(m_fsm.on_timeout());
        });
    }

    peer_context<Policy> &m_ctx;
    channel_type &m_channel;
    handshake_fsm_config m_fsm_cfg;
    handshake_fsm m_fsm;
    frame_router m_router;
    timer_type m_handshake_timer;
    message_forwarder<Policy> &m_messages;
    procedure_forwarder<Policy> &m_procedures;
    std::chrono::nanoseconds m_handshake_timeout;
    bool m_is_inbound_bootstrap;
    std::uint64_t m_session_id{0}, m_peer_session_id{0};
    bool m_forwarders_installed{false}, m_torn_down{false};
    // Latched true by close_for_protocol_error so the on_error wiring short-circuits
    // the re-dial: a peer WE closed for misbehavior must not be re-dialed. A plain
    // bool, mirroring m_torn_down — its absence is not meaningful, only its value.
    bool m_closed_for_protocol_error{false};
    // The readiness counter + fire-once latch, OWNED by the per-incarnation session
    // (cleared in tear_down so reconnect re-arms — a fresh session is auto-rearmed at
    // 0). The counter is the number of outstanding subscribe acks; the latch records
    // whether ready already fired this cycle. Plain values, mirroring m_torn_down —
    // their absence is not meaningful, only their value.
    std::uint16_t m_outstanding_subscribes{0};
    bool m_ready_latched_this_cycle{false};
    log::logger &m_logger;
    typename message_forwarder<Policy>::peer m_msg_peer;
    typename procedure_forwarder<Policy>::peer m_rpc_peer;
    std::vector<std::byte> m_payload_scratch, m_frame_scratch;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>)> m_on_message;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_with_info;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_route;
    plexus::detail::move_only_function<void()> m_on_drop;
    plexus::detail::move_only_function<void(const lifecycle_event &)> m_on_lifecycle;
    plexus::detail::move_only_function<void(const node_id &)> m_on_stamp_seen;
    plexus::detail::move_only_function<void(const security_event &)> m_on_security;
    plexus::detail::move_only_function<void(const security_negotiation &)> m_install_security;
    // The borrowed node-level OpenSSL-free seam (the transcript digest); the per-session
    // install hook above captures the channel. The pending attach holds the assembled
    // facts + negotiation across the gate decision and the accept install; the
    // authenticated host identity latches at a security-engaged accept (absent otherwise).
    const security_seam *m_install_security_seam{nullptr};
    pending_attach m_pending_attach;
    std::optional<node_id> m_authenticated_host_identity;
    // The per-session attach credentials: own_nonce is minted once from the rand_fn seam
    // (a fresh key-schedule salt per session — no all-zero nonce), key_id is adopted from
    // the engaged prover. m_peer_proof is the stable backing buffer the decoded wire proof
    // is latched into so the facts.proof span outlives the synchronous gate decide().
    std::array<std::byte, 16> m_own_nonce{};
    std::array<std::byte, wire::k_handshake_key_id_len> m_key_id{};
    std::array<std::byte, wire::k_handshake_proof_len> m_peer_proof{};
    security::rand_fn m_rand;
    security::attach_prover m_prover;
    plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> m_on_subscribe_refused;
    plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> m_on_subscribe_degraded;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_H

#include "plexus/io/null_logger.h"
#include "plexus/io/frame_router.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <string_view>
#include <system_error>

namespace plexus::io {

// The per-peer bridge: one live connection turned into a session. It drives the
// pure handshake_fsm over an ALREADY-LIVE channel (the ctor performs no dial or
// listen so a future transport backend slots in unchanged), owns a per-peer
// frame_router whose consumers decode handshake frames into the FSM and hand data
// frames to the node-shared forwarders carrying THIS peer's identity, mints a
// per-connection session_id epoch on completion, and runs the receive-path
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

    // handshake_timeout is required with NO default — a session cannot arm a bound
    // it was not told, and the right value depends on the deployment.
    peer_session(channel_type &channel, executor_type executor,
                 const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout,
                 message_forwarder<Policy> &messages, procedure_forwarder<Policy> &procedures,
                 std::string node_name, bool is_inbound_bootstrap,
                 log::logger &logger = shared_null_logger())
        : m_channel(channel), m_fsm_cfg(fsm_cfg), m_fsm(fsm_cfg), m_handshake_timer(executor)
        , m_messages(messages), m_procedures(procedures), m_node_name(std::move(node_name))
        , m_handshake_timeout(handshake_timeout), m_is_inbound_bootstrap(is_inbound_bootstrap)
        , m_logger(logger), m_msg_peer{m_channel, m_node_name}, m_rpc_peer{m_channel, m_node_name}
    {
    }

    // Wire the receive path, register the router consumers, arm the handshake
    // timer, and (for an outbound peer) begin the handshake. Two paired sessions
    // both begin outbound: the simultaneous-connect path completes on both ends.
    void start()
    {
        m_torn_down = false;
        m_channel.on_data([this](std::span<const std::byte> f) { on_receive(f); });
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

    // The staleness gate runs BEFORE the router: a frame whose non-zero session_id
    // differs from the latched epoch is a previous-session straggler and is dropped;
    // the first non-zero observation latches the peer's epoch. A frame already posted
    // before tear_down is ignored (no phantom completion on a closed session).
    void on_receive(std::span<const std::byte> frame)
    {
        if(m_torn_down)
            return;
        if(auto hdr = wire::decode_header(frame))
        {
            if(hdr->session_id != 0 && m_peer_session_id != 0 && hdr->session_id != m_peer_session_id)
                return;
            if(hdr->session_id != 0 && m_peer_session_id == 0)
                m_peer_session_id = hdr->session_id;
        }
        m_router.route(frame);
    }

    // Cancel the timer, detach the forwarders for this peer, reset the epoch latch
    // and the FSM for a fresh cycle, and close the channel.
    void tear_down()
    {
        m_torn_down = true;
        m_handshake_timer.cancel();
        m_messages.detach_all(m_msg_peer);
        m_procedures.detach_all(m_rpc_peer);
        m_peer_session_id = 0;
        m_forwarders_installed = false;
        m_fsm.on_torn_down();
        m_channel.close();
    }

    bool is_complete() const noexcept { return m_forwarders_installed; }
    std::uint8_t session_id() const noexcept { return m_session_id; }
    std::uint8_t peer_session_id() const noexcept { return m_peer_session_id; }
    const typename message_forwarder<Policy>::peer &msg_peer() const noexcept { return m_msg_peer; }
    const typename procedure_forwarder<Policy>::peer &rpc_peer() const noexcept { return m_rpc_peer; }

private:
    void register_consumers()
    {
        m_router.on_handshake_req([this](std::span<const std::byte> inner) {
            if(auto req = wire::decode_handshake_request(inner))
                execute(m_fsm.on_request(*req, m_is_inbound_bootstrap));
        });
        m_router.on_handshake_resp([this](std::span<const std::byte> inner) {
            if(auto resp = wire::decode_handshake_response(inner))
                execute(m_fsm.on_response(*resp));
        });
        m_router.on_unidirectional([this](std::span<const std::byte> inner) {
            m_messages.deliver(m_msg_peer, inner,
                               [this](std::string_view fqn, std::span<const std::byte> data) {
                                   if(m_on_message)
                                       m_on_message(fqn, data);
                               });
        });
        m_router.on_rpc_request([this](std::span<const std::byte> inner) {
            m_procedures.deliver_request(m_rpc_peer, inner, m_session_id);
        });
        m_router.on_rpc_response([this](std::span<const std::byte> inner) {
            m_procedures.deliver_response(m_rpc_peer, inner);
        });
    }

    // Map an FSM action to channel I/O. retry has no dialer this block — a
    // no-op-with-log; the reconnect path is a later concern.
    void execute(const fsm_step_result &step)
    {
        switch(step.action)
        {
            case fsm_action::send_request:  return send_handshake_request();
            case fsm_action::send_response: return send_handshake_response(step.outcome);
            case fsm_action::complete:      return on_complete();
            case fsm_action::retry:         return m_logger.warn("plexus: peer_session retry_no_dialer");
            case fsm_action::abort:         return tear_down();
            case fsm_action::none:          return;
        }
    }

    // The response reuses every request field and adds a status, so this is the one
    // place the self identity/versions are stamped onto the wire.
    wire::handshake_request self_request() const noexcept
    {
        return {.id                       = m_fsm_cfg.self_id,
                .version_major            = m_fsm_cfg.version_major,
                .version_minor            = m_fsm_cfg.version_minor,
                .compatible_version_major = m_fsm_cfg.compatible_version_major,
                .compatible_version_minor = m_fsm_cfg.compatible_version_minor,
                .protocol_version         = wire::k_protocol_version};
    }

    void send_handshake_request()
    {
        wire::encode_handshake_request_into(m_payload_scratch, self_request());
        send_control(wire::msg_type::handshake_req);
    }

    void send_handshake_response(handshake_outcome outcome)
    {
        const auto r = self_request();
        wire::handshake_response resp{.id = r.id, .version_major = r.version_major,
                .version_minor = r.version_minor, .compatible_version_major = r.compatible_version_major,
                .compatible_version_minor = r.compatible_version_minor,
                .protocol_version = r.protocol_version, .status = status_for(outcome)};
        wire::encode_handshake_response_into(m_payload_scratch, resp);
        send_control(wire::msg_type::handshake_resp);
    }

    static wire::handshake_status status_for(handshake_outcome outcome) noexcept
    {
        switch(outcome)
        {
            case handshake_outcome::reject_version:  return wire::handshake_status::version_incompatible;
            case handshake_outcome::reject_identity: return wire::handshake_status::identity_conflict;
            default:                                 return wire::handshake_status::accepted;
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
    void on_complete()
    {
        if(m_forwarders_installed)
            return;
        m_handshake_timer.cancel();
        mint_session_id();
        m_forwarders_installed = true;
    }

    void mint_session_id() noexcept
    {
        ++m_session_id_counter;
        if(m_session_id_counter == 0)
            m_session_id_counter = 1;
        m_session_id = m_session_id_counter;
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

    channel_type &m_channel;
    handshake_fsm_config m_fsm_cfg;
    handshake_fsm m_fsm;
    frame_router m_router;
    timer_type m_handshake_timer;
    message_forwarder<Policy> &m_messages;
    procedure_forwarder<Policy> &m_procedures;
    std::string m_node_name;
    std::chrono::nanoseconds m_handshake_timeout;
    bool m_is_inbound_bootstrap;
    std::uint8_t m_session_id{0}, m_peer_session_id{0}, m_session_id_counter{0};
    bool m_forwarders_installed{false}, m_torn_down{false};
    log::logger &m_logger;
    typename message_forwarder<Policy>::peer m_msg_peer;
    typename procedure_forwarder<Policy>::peer m_rpc_peer;
    std::vector<std::byte> m_payload_scratch, m_frame_scratch;
    detail::move_only_function<void(std::string_view, std::span<const std::byte>)> m_on_message;
};

}

#endif

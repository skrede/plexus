#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/frame_router.h"
#include "plexus/io/message_info.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/attach_negotiator.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/subscribe_retry_config.h"

#include "plexus/io/detail/warn_line.h"
#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/session_hooks.h"
#include "plexus/io/detail/peer_session_retry.h"
#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_consumers.h"
#include "plexus/io/detail/peer_session_handshake.h"
#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/io/security/attach_policy.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::io {

template<typename Policy>
    requires plexus::Policy<Policy>
class peer_session : public detail::session_hooks
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;
    using clock_type    = plexus::timer_clock_t<timer_type>;

    peer_session(peer_context<Policy> &ctx, executor_type executor, const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout, message_forwarder<Policy> &messages,
                 procedure_forwarder<Policy> &procedures, bool is_inbound_bootstrap, log::logger &logger)
            : m_torn_down(false)
            , m_fsm(fsm_cfg)
            , m_router(logger)
            , m_quiet_teardown(false)
            , m_logger(logger)
            , m_channel(*ctx.channel)
            , m_from_intra_process(tier_of(m_channel.remote_endpoint().scheme) == locality::process)
            , m_session_id(0)
            , m_forwarders_installed(false)
            , m_is_inbound_bootstrap(is_inbound_bootstrap)
            , m_ctx(ctx)
            , m_waits()
            , m_session_timer(executor)
            , m_fsm_cfg(fsm_cfg)
            , m_ready_latched_this_cycle(false)
            , m_peer_session_id(0)
            , m_closed_for_protocol_error(false)
            , m_messages(messages)
            , m_outstanding_subscribes(0)
            , m_procedures(procedures)
            , m_retry()
            , m_handshake_timeout(handshake_timeout)
            , m_msg_peer{m_channel, ctx.node_name}
            , m_malformed_warning(detail::k_malformed_warning_interval)
            , m_rpc_peer{m_channel, ctx.node_name}
    {
    }

    void start()
    {
        detail::start_session(*this);
    }

    io::detail::pending_ops &pending() noexcept
    {
        return m_waits;
    }

    void set_declines_relay(bool declines) noexcept
    {
        m_declines_relay = declines;
    }

    void on_install_security(plexus::detail::move_only_function<void(const security_negotiation &)> cb)
    {
        m_negotiator.on_install_security(std::move(cb));
    }

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

    std::optional<node_id> authenticated_host_identity() const noexcept
    {
        return m_negotiator.authenticated_host_identity();
    }

    void on_receive(std::span<const std::byte> frame)
    {
        detail::receive_frame(*this, frame);
    }

    void inject_receive(std::span<const std::byte> frame)
    {
        on_receive(frame);
    }

    void tear_down()
    {
        detail::tear_down_session(*this);
    }

    void on_channel_protocol_close(wire::close_cause cause)
    {
        detail::protocol_close(*this, cause);
    }

    void close_for_protocol_error(wire::close_cause cause)
    {
        detail::close_for_protocol_error(*this, cause);
    }

    // Set before start(): a subscribe already outstanding is untracked, so its response would be ignored.
    void set_subscribe_retry(const subscribe_retry_config &cfg)
    {
        m_retry.config = cfg;
    }

    void subscribe(std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt, std::string_view type_name = {})
    {
        if(!m_messages.attach(m_msg_peer, fqn, qos, type_id, type_name))
            return;
        ++m_outstanding_subscribes;
        detail::note_subscribe_sent(*this, wire::fqn_topic_hash(fqn));
    }

    void unsubscribe(std::string_view fqn)
    {
        m_messages.detach(m_msg_peer, fqn);
    }

    void declare(const wire::topic_declaration &td)
    {
        m_messages.send_declare(m_channel, td);
    }

    void emit_heartbeat()
    {
        wire::heartbeat hb;
        if(m_declines_relay)
            hb.reserved |= wire::k_heartbeat_relay_decline_flag;
        wire::encode_heartbeat_into(m_payload_scratch, hb);
        detail::send_control(*this, wire::msg_type::heartbeat);
    }

    void maybe_fire_ready()
    {
        if(m_outstanding_subscribes == 0 && !m_ready_latched_this_cycle)
        {
            m_ready_latched_this_cycle = true;
            detail::fire_lifecycle(*this, lifecycle_edge::ready);
        }
    }

    bool is_complete() const noexcept
    {
        return m_forwarders_installed;
    }

    // The identity the peer proved in the handshake. It differs from the slot's key on an accepted
    // session, whose provisional id was minted before the peer spoke.
    const node_id &peer_identity() const noexcept
    {
        return m_fsm.last_seen_peer_id();
    }

    bool same_host() const noexcept
    {
        return m_ctx.same_host;
    }

    std::uint64_t session_id() const noexcept
    {
        return m_session_id;
    }

    std::uint64_t peer_session_id() const noexcept
    {
        return m_peer_session_id;
    }

    const typename message_forwarder<Policy>::peer &msg_peer() const noexcept
    {
        return m_msg_peer;
    }

    const typename procedure_forwarder<Policy>::peer &rpc_peer() const noexcept
    {
        return m_rpc_peer;
    }

private:
    bool m_torn_down;
    handshake_fsm m_fsm;
    frame_router m_router;
    bool m_quiet_teardown;
    log::logger &m_logger;
    channel_type &m_channel;
    bool m_from_intra_process;
    std::uint64_t m_session_id;
    bool m_forwarders_installed;
    bool m_is_inbound_bootstrap;
    peer_context<Policy> &m_ctx;
    io::detail::pending_ops m_waits;
    timer_type m_session_timer;
    handshake_fsm_config m_fsm_cfg;
    bool m_ready_latched_this_cycle;
    std::uint64_t m_peer_session_id;
    bool m_closed_for_protocol_error;
    message_forwarder<Policy> &m_messages;
    attach_negotiator<Policy> m_negotiator;
    std::vector<std::byte> m_frame_scratch;
    std::uint16_t m_outstanding_subscribes;
    std::vector<std::byte> m_payload_scratch;
    procedure_forwarder<Policy> &m_procedures;
    io::detail::subscribe_retry_state m_retry;
    std::chrono::nanoseconds m_handshake_timeout;
    typename message_forwarder<Policy>::peer m_msg_peer;
    io::detail::rate_limited_warning<clock_type> m_malformed_warning;
    typename procedure_forwarder<Policy>::peer m_rpc_peer;
    bool m_declines_relay = false;

    template<typename S>
    friend void detail::register_session_consumers(S &);
    template<typename S>
    friend void detail::register_handshake_consumers(S &);
    template<typename S>
    friend void detail::fold_topic_edge(S &, std::string_view, std::string_view, std::optional<std::uint64_t>, graph::topic_role);
    template<typename S>
    friend void detail::on_subscribe_received(S &, std::span<const std::byte>);
    template<typename S>
    friend bool detail::deliver_session_data(S &, const wire::frame_header &, std::span<const std::byte>);
    template<typename S>
    friend bool detail::deliver_data_with_source(S &, const wire::frame_header &, std::span<const std::byte>, const node_id &);
    template<typename S>
    friend void detail::deliver_forwarded_frame(S &, const node_id &, std::span<const std::byte>);
    template<typename S>
    friend bool detail::dispatch_forwarded_inner(S &, const wire::forwarded_frame &, const wire::frame_header &);
    template<typename S>
    friend void detail::refan_if_pubsub(S &, const wire::forwarded_frame &, const wire::frame_header &);
    template<typename S>
    friend void detail::originate_if_pubsub(S &, const wire::frame_header &, std::span<const std::byte>);
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
    friend void detail::redeclare_all(S &);
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
    template<typename S>
    friend void detail::tear_down_session(S &);
    template<typename S>
    friend void detail::quiet_tear_down(S &);
    template<typename S>
    friend bool detail::is_demoted(const S &) noexcept;
    template<typename S>
    friend void detail::demote_loser(S &);
    template<typename S>
    friend void detail::close_for_protocol_error(S &, wire::close_cause);
    template<typename S>
    friend void detail::protocol_close(S &, wire::close_cause);
    template<typename S>
    friend void detail::receive_frame(S &, std::span<const std::byte>);
    template<typename S>
    friend void detail::report_drop(S &, detail::drop_cause, std::uint64_t);
    template<typename S>
    friend void detail::report_malformed(S &);
    template<typename S>
    friend void detail::arm_handshake_timer(S &);
    template<typename S>
    friend void detail::note_admitted(S &, const wire::frame_header &);
    template<typename S>
    friend void detail::note_subscribe_sent(S &, std::uint64_t);
    template<typename S>
    friend void detail::arm_subscribe_retry(S &);
    template<typename S>
    friend void detail::on_subscribe_retry_due(S &);
    template<typename S>
    friend bool detail::is_settled(const S &, const detail::unacked_subscribe &);
    template<typename S>
    friend void detail::resend_subscribe(S &, detail::unacked_subscribe &);
    template<typename S>
    friend void detail::settle_subscribe(S &, const detail::unacked_subscribe &);
    template<typename S>
    friend void detail::exhaust_subscribe(S &, const detail::unacked_subscribe &);
    template<typename S>
    friend void detail::release_subscribe(S &);
    template<typename S>
    friend void detail::redeclare_once(S &);
    template<typename S>
    friend void detail::wire_channel_seams(S &);
    template<typename S>
    friend void detail::start_session(S &);
};

}

#endif

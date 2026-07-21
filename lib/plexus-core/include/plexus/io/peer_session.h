#ifndef HPP_GUARD_PLEXUS_IO_PEER_SESSION_H
#define HPP_GUARD_PLEXUS_IO_PEER_SESSION_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/io_error.h"
#include "plexus/io/locality.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/frame_router.h"
#include "plexus/io/message_info.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/host_identity.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/attach_negotiator.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/io/detail/peer_session_deliver.h"
#include "plexus/io/detail/peer_session_complete.h"
#include "plexus/io/detail/peer_session_consumers.h"
#include "plexus/io/detail/peer_session_handshake.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/topic_declaration.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/fetch_latched.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace plexus::io {

template<typename Policy>
    requires plexus::Policy<Policy>
class peer_session
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;

    peer_session(peer_context<Policy> &ctx, executor_type executor, const handshake_fsm_config &fsm_cfg, std::chrono::nanoseconds handshake_timeout, message_forwarder<Policy> &messages,
                 procedure_forwarder<Policy> &procedures, bool is_inbound_bootstrap, log::logger &logger)
            : m_torn_down(false)
            , m_fsm(fsm_cfg)
            , m_router(logger)
            , m_logger(logger)
            , m_channel(*ctx.channel)
            , m_from_intra_process(tier_of(m_channel.remote_endpoint().scheme) == locality::process)
            , m_session_id(0)
            , m_forwarders_installed(false)
            , m_is_inbound_bootstrap(is_inbound_bootstrap)
            , m_ctx(ctx)
            , m_handshake_timer(executor)
            , m_fsm_cfg(fsm_cfg)
            , m_ready_latched_this_cycle(false)
            , m_peer_session_id(0)
            , m_closed_for_protocol_error(false)
            , m_messages(messages)
            , m_outstanding_subscribes(0)
            , m_procedures(procedures)
            , m_handshake_timeout(handshake_timeout)
            , m_msg_peer{m_channel, ctx.node_name}
            , m_rpc_peer{m_channel, ctx.node_name}
    {
    }

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
                    // would_block is a transient back-pressure stall, not a broken channel; only a
                    // genuine channel break drives the transport-drop reconnect.
                    if(e == io_error::would_block)
                        return;
                    if(m_on_drop_cb && !m_torn_down && !m_closed_for_protocol_error)
                        m_on_drop_cb();
                });
        m_channel.on_protocol_close([this](wire::close_cause cause) { on_channel_protocol_close(cause); });
        if constexpr(requires(channel_type &c) { c.on_object([](const object_carrier &) {}); })
            m_channel.on_object([this](const object_carrier &c) { detail::deliver_session_object(*this, c); });
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
        m_on_message_cb = std::move(on_message);
    }

    template<typename OnMessageWithInfo>
    void on_message_with_info(OnMessageWithInfo cb)
    {
        m_on_message_with_info_cb = std::move(cb);
    }

    void on_message_route(plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> cb)
    {
        m_on_message_route_cb = std::move(cb);
    }

    void on_object_route(plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> cb)
    {
        m_on_object_route_cb = std::move(cb);
    }

    void on_transport_drop(plexus::detail::move_only_function<void()> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    void on_lifecycle(plexus::detail::move_only_function<void(const lifecycle_event &)> cb)
    {
        m_on_lifecycle_cb = std::move(cb);
    }

    void on_stamp_seen(plexus::detail::move_only_function<void(const node_id &)> cb)
    {
        m_on_stamp_seen_cb = std::move(cb);
    }

    void on_security(plexus::detail::move_only_function<void(const security_event &)> cb)
    {
        m_on_security_cb = std::move(cb);
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

    void on_subscribe_refused(plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> cb)
    {
        m_on_subscribe_refused_cb = std::move(cb);
    }

    void on_subscribe_degraded(plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> cb)
    {
        m_on_subscribe_degraded_cb = std::move(cb);
    }

    // The staleness gate runs before the router: a frame whose non-zero session_id differs from
    // the latched epoch is a previous-session straggler and is dropped; the first non-zero
    // observation latches the peer's epoch.
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

    void inject_receive(std::span<const std::byte> frame)
    {
        on_receive(frame);
    }

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

    void on_channel_protocol_close(wire::close_cause cause)
    {
        if(m_torn_down)
            return;
        if(m_negotiator.engaged() && m_forwarders_installed)
            detail::fire_security(*this, security_kind::stream_tamper_teardown, security_cause::tag_verify_failed);
        close_for_protocol_error(cause);
    }

    void close_for_protocol_error(wire::close_cause)
    {
        if(m_torn_down)
            return;
        m_closed_for_protocol_error = true;
        m_logger.warn("plexus: peer_session protocol_close");
        tear_down();
    }

    void subscribe(std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt, std::string_view type_name = {})
    {
        if(m_messages.attach(m_msg_peer, fqn, qos, type_id, type_name))
            ++m_outstanding_subscribes;
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
        wire::encode_heartbeat_into(m_payload_scratch, wire::heartbeat{});
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
    log::logger &m_logger;
    channel_type &m_channel;
    bool m_from_intra_process;
    std::uint64_t m_session_id;
    bool m_forwarders_installed;
    bool m_is_inbound_bootstrap;
    peer_context<Policy> &m_ctx;
    timer_type m_handshake_timer;
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
    std::chrono::nanoseconds m_handshake_timeout;
    typename message_forwarder<Policy>::peer m_msg_peer;
    typename procedure_forwarder<Policy>::peer m_rpc_peer;
    plexus::detail::move_only_function<void()> m_on_drop_cb;
    plexus::detail::move_only_function<void(const node_id &)> m_on_stamp_seen_cb;
    plexus::detail::move_only_function<void(const security_event &)> m_on_security_cb;
    plexus::detail::move_only_function<void(const lifecycle_event &)> m_on_lifecycle_cb;
    plexus::detail::move_only_function<void(std::uint64_t, std::uint8_t)> m_on_subscribe_degraded_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>)> m_on_message_cb;
    plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> m_on_object_route_cb;
    plexus::detail::move_only_function<void(std::uint64_t, wire::subscribe_status)> m_on_subscribe_refused_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_route_cb;
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> m_on_message_with_info_cb;

    template<typename S>
    friend void detail::register_session_consumers(S &);
    template<typename S>
    friend void detail::fold_topic_edge(S &, std::string_view, std::string_view, std::optional<std::uint64_t>, graph::topic_role);
    template<typename S>
    friend void detail::on_subscribe_received(S &, std::span<const std::byte>);
    template<typename S>
    friend void detail::deliver_session_data(S &, const wire::frame_header &, std::span<const std::byte>);
    template<typename S>
    friend void detail::deliver_data_with_source(S &, const wire::frame_header &, std::span<const std::byte>, const node_id &);
    template<typename S>
    friend void detail::deliver_forwarded_frame(S &, const node_id &, std::span<const std::byte>);
    template<typename S>
    friend void detail::dispatch_forwarded_inner(S &, const wire::forwarded_frame &);
    template<typename S>
    friend void detail::refan_if_pubsub(S &, const wire::forwarded_frame &);
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

    void arm_handshake_timer()
    {
        m_handshake_timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(m_handshake_timeout));
        m_handshake_timer.async_wait(
                [this](std::error_code ec)
                {
                    if(ec)
                        return;
                    const auto step = m_fsm.on_timeout();
                    detail::execute(*this, step);
                    // A bounded handshake re-send keeps the window open: re-arm here so a subsequent
                    // loss is also bounded. arm_handshake_timer stays the single source of the window.
                    if(step.action == fsm_action::send_request)
                        arm_handshake_timer();
                });
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/node_name.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"

#include "plexus/io/detail/procedure_fanout.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>
#include <unordered_map>

namespace plexus::io {

// The req/res sibling of message_forwarder: a Policy-templated forwarder that correlates opaque-byte
// requests to responses by a monotonic correlation_id, tracking pending calls in a bounded per-peer
// outstanding-request table (an over-capacity call fails fast). An unmatched response is dropped as
// an orphan. It arms a Policy timer per call (a forwarder default deadline, optionally overridden):
// on expiry the entry fires rpc_status::timeout; a matched response or detach_all cancels it first.
// It exposes deliver_request/deliver_response and owns no frame_header.type switch — the frame_router
// owns the type switch and hands each the inner header-off payload.
template<typename Policy>
    requires plexus::Policy<Policy>
class procedure_forwarder
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using executor_type = typename Policy::executor_type;
    using timer_type    = typename Policy::timer_type;
    using endpoint_type = subscription_endpoint<channel_type>;
    using peer          = typename endpoint_type::peer;

    using reply_fn = plexus::detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    using handler_fn = plexus::detail::move_only_function<void(std::span<const std::byte> param, reply_fn &)>;

    using on_response_fn = plexus::detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    using forward_rpc_fn = plexus::detail::move_only_function<bool(const node_id &destination, std::span<const std::byte> inner_frame)>;

    static constexpr std::size_t k_default_max_outstanding = 1024;

    procedure_forwarder(executor_type executor, std::chrono::nanoseconds default_deadline, log::logger &logger, std::size_t max_outstanding = k_default_max_outstanding)
            : m_logger(logger)
            , m_executor(executor)
            , m_max_outstanding(max_outstanding)
            , m_active_channel{nullptr}
            , m_active_session_id{0}
            , m_next_correlation_id{1}
            , m_default_deadline(default_deadline)
            , m_active_req_hdr{}
    {
    }

    void on_rpc_call(plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> hook)
    {
        m_on_rpc_call = std::move(hook);
    }

    void on_rpc_serve(plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> hook)
    {
        m_on_rpc_serve = std::move(hook);
    }

    void on_rpc_reply(plexus::detail::move_only_function<void(std::string_view, const rpc_reply_view &)> hook)
    {
        m_on_rpc_reply = std::move(hook);
    }

    // The engine seam a caller's forwarded request and a responder's forwarded reply both emit through:
    // given a destination identity and a header-on rpc frame, the engine route_selects over the
    // destination's candidates and sends the wrapped frame onto the chosen via-session. Absent (a bare
    // forwarder with no engine), a forwarded call/reply cannot leave and its deadline fires the timeout.
    void on_forward_rpc(forward_rpc_fn cb)
    {
        m_forward_rpc = std::move(cb);
    }

    void serve(std::string_view fqn, handler_fn handler)
    {
        m_hash_to_fqn[wire::fqn_topic_hash(fqn)] = std::string{fqn};
        m_providers[std::string{fqn}]            = std::move(handler);
    }

    void retire(std::string_view fqn)
    {
        m_hash_to_fqn.erase(wire::fqn_topic_hash(fqn));
        m_providers.erase(std::string{fqn});
    }

    void call(const peer &p, std::string_view fqn, std::span<const std::byte> param, on_response_fn on_response, std::optional<std::chrono::nanoseconds> deadline = std::nullopt,
              std::uint64_t session_id = 0)
    {
        auto &per_peer = m_outstanding[p.node_name];
        if(per_peer.size() >= m_max_outstanding)
            return on_response(wire::rpc_status::error, {});

        std::uint64_t corr_id = m_next_correlation_id++;
        wire::encode_rpc_request_into(m_req_scratch, request_header(fqn, corr_id), param);
        detail::send_data(*this, p.channel, wire::msg_type::rpc_request, m_req_scratch, session_id);
        detail::emit_rpc_call(*this, fqn, corr_id);

        auto [it, _] = per_peer.emplace(corr_id, pending_rpc{std::string{fqn}, std::move(on_response), std::make_unique<timer_type>(m_executor)});
        arm_deadline(p.node_name, corr_id, *it->second.timer, deadline.value_or(m_default_deadline));
    }

    // The relayed sibling of call(): with no direct session to the responder, mint the correlation, build
    // a header-on rpc_request frame, and hand it to the engine's route-resolution seam (which wraps it in
    // a forwarded envelope addressed to the responder and sends it onto a route_select-chosen via-session).
    // The pending entry keys on the responder's IDENTITY, not the via, so a reply re-resolving back through
    // a different relay still matches. An unroutable request still arms the deadline — the caller fails via
    // rpc_status::timeout, never a manufactured route.
    void forward_call(const node_id &destination, std::string_view fqn, std::span<const std::byte> param, on_response_fn on_response,
                      std::optional<std::chrono::nanoseconds> deadline = std::nullopt)
    {
        const std::string key = node_name_of(destination);
        auto &per_peer        = m_outstanding[key];
        if(per_peer.size() >= m_max_outstanding)
            return on_response(wire::rpc_status::error, {});

        std::uint64_t corr_id = m_next_correlation_id++;
        wire::encode_rpc_request_into(m_req_scratch, request_header(fqn, corr_id), param);
        stage_rpc_frame(wire::msg_type::rpc_request, m_req_scratch);
        if(m_forward_rpc)
            m_forward_rpc(destination, m_frame_scratch);
        detail::emit_rpc_call(*this, fqn, corr_id);

        auto [it, _] = per_peer.emplace(corr_id, pending_rpc{std::string{fqn}, std::move(on_response), std::make_unique<timer_type>(m_executor)});
        arm_deadline(key, corr_id, *it->second.timer, deadline.value_or(m_default_deadline));
    }

    // Per-(peer, fqn) refcount gate: on 0->1 it emits a subscribe. A call() needs no prior attach.
    bool attach(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name);
        detail::send_subscribe(*this, p.channel, fqn, hash);
        return true;
    }

    // The same gate as attach, but emits a subscribe_response (the reaction to an arriving subscribe).
    bool attach_for_fanout(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name);
        auto resp = wire::encode_subscribe_response({.topic_hash = hash, .status = wire::subscribe_status::subscribed});
        send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return true;
    }

    // Per-(peer, fqn) refcount gate: on 1->0 it emits an unsubscribe.
    bool detach(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.detach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().remove_subscriber(hash, p.channel);
        auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
        send_control(p.channel, wire::msg_type::unsubscribe, req);
        return true;
    }

    // The peer-death resolution path: cancel each entry's timer (so a torn-down entry fires no late
    // timeout), fire every pending callback with peer_disconnected, and drop the peer's entries.
    void detach_all(const peer &p)
    {
        m_endpoint.remove_peer(p);

        auto peer_it = m_outstanding.find(p.node_name);
        if(peer_it == m_outstanding.end())
            return;

        // Move the peer's pending table out and drop the map entry BEFORE firing any callback: an
        // on_response may re-issue a call that inserts into m_outstanding — rehashing the outer map
        // (invalidating peer_it) or mutating this peer's inner map under the iterator. Firing from the
        // detached copy leaves no live iterator into a container the callback can reach, mirroring
        // deliver_response / fire_timeout.
        auto detached = std::move(peer_it->second);
        m_outstanding.erase(peer_it);
        for(auto &[corr_id, pending] : detached)
        {
            pending.timer->cancel();
            pending.on_response(wire::rpc_status::peer_disconnected, {});
        }
    }

    // Resolve a local provider by topic_hash and dispatch to it over the opaque param bytes. An
    // unserved hash replies no_handler, unless the subscriber registry knows the fqn from a prior
    // attach (a known topic whose provider is gone), which replies topic_not_found instead.
    void deliver_request(const peer &p, std::span<const std::byte> inner, std::uint64_t session_id = 0, const node_id *reply_origin = nullptr)
    {
        auto decoded = wire::decode_rpc_request(inner);
        if(!decoded)
            return drop("plexus: forwarder rpc_request decode_failed");

        const auto req_hdr       = decoded->header;
        m_active_channel         = &p.channel;
        m_active_req_hdr         = req_hdr;
        m_active_session_id      = session_id;
        m_active_reply_forwarded = reply_origin != nullptr;
        if(reply_origin)
            m_active_reply_origin = *reply_origin;

        auto hash_it = m_hash_to_fqn.find(req_hdr.topic_hash);
        if(hash_it == m_hash_to_fqn.end())
        {
            auto status = m_endpoint.registry().fqn_for(req_hdr.topic_hash).empty() ? wire::rpc_status::no_handler : wire::rpc_status::topic_not_found;
            return emit_reply(status, {});
        }
        ensure_reply_ready();
        detail::emit_rpc_serve(*this, hash_it->second, req_hdr.correlation_id);
        m_providers.find(hash_it->second)->second(decoded->param_data, m_reply);
    }

    // Find the pending entry for this peer by correlation_id, move it out + erase it, and fire its
    // callback. A non-matching corr_id is dropped as an orphan and fires no callback.
    // key_override addresses the pending table by an identity other than the arrival session's peer: a
    // forwarded reply arrives over whatever relay re-resolution chose, so it keys on the responder ORIGIN
    // (carried by the forwarded envelope) rather than the via, matching forward_call's origin-keyed entry.
    void deliver_response(const peer &p, std::span<const std::byte> inner, const std::string *key_override = nullptr)
    {
        auto decoded = wire::decode_rpc_response(inner);
        if(!decoded)
            return drop("plexus: forwarder rpc_response decode_failed");

        auto peer_it = m_outstanding.find(key_override ? *key_override : p.node_name);
        if(peer_it == m_outstanding.end())
            return drop("plexus: forwarder rpc_response_orphan");

        auto entry_it = peer_it->second.find(decoded->header.correlation_id);
        if(entry_it == peer_it->second.end())
            return drop("plexus: forwarder rpc_response_orphan");

        pending_rpc pending = std::move(entry_it->second);
        peer_it->second.erase(entry_it);
        pending.timer->cancel();
        detail::emit_rpc_reply(*this, pending.fqn, decoded->header.correlation_id, decoded->status);
        pending.on_response(decoded->status, decoded->return_data);
    }

    // Introspection: the total pending calls across peers. A relay that only re-forwards holds none, so
    // this stays zero on the relay reply path — the stateless-reply invariant, observable in a test.
    std::size_t outstanding() const noexcept
    {
        std::size_t n = 0;
        for(const auto &[name, per_peer] : m_outstanding)
            n += per_peer.size();
        return n;
    }

private:
    struct pending_rpc
    {
        std::string fqn;
        on_response_fn on_response;
        std::unique_ptr<timer_type> timer;
    };

    reply_fn m_reply;
    log::logger &m_logger;
    endpoint_type m_endpoint;
    executor_type m_executor;
    std::size_t m_max_outstanding;
    channel_type *m_active_channel;
    std::uint64_t m_active_session_id;
    std::uint64_t m_next_correlation_id;
    std::vector<std::byte> m_req_scratch;
    std::vector<std::byte> m_resp_scratch;
    std::vector<std::byte> m_frame_scratch;
    std::chrono::nanoseconds m_default_deadline;
    wire::bidirectional_header m_active_req_hdr;
    bool m_active_reply_forwarded{false};
    node_id m_active_reply_origin{};
    forward_rpc_fn m_forward_rpc;
    std::unordered_map<std::string, handler_fn> m_providers;
    std::unordered_map<std::uint64_t, std::string> m_hash_to_fqn;
    std::unordered_map<std::string, std::unordered_map<std::uint64_t, pending_rpc>> m_outstanding;
    plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> m_on_rpc_call;
    plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> m_on_rpc_serve;
    plexus::detail::move_only_function<void(std::string_view, const rpc_reply_view &)> m_on_rpc_reply;

    template<typename F>
    friend void detail::emit_rpc_call(F &, std::string_view, std::uint64_t);
    template<typename F>
    friend void detail::emit_rpc_serve(F &, std::string_view, std::uint64_t);
    template<typename F>
    friend void detail::emit_rpc_reply(F &, std::string_view, std::uint64_t, wire::rpc_status);
    template<typename F, typename C>
    friend void detail::reply_status(F &, C &, const wire::bidirectional_header &, wire::rpc_status, std::span<const std::byte>, std::uint64_t);
    template<typename F, typename C>
    friend void detail::send_data(F &, C &, wire::msg_type, std::span<const std::byte>, std::uint64_t);
    template<typename F, typename C>
    friend void detail::send_subscribe(F &, C &, std::string_view, std::uint64_t);

    // The handler looks the entry up by (node_name, corr_id) rather than capturing it, which moves.
    void arm_deadline(const std::string &node_name, std::uint64_t corr_id, timer_type &timer, std::chrono::nanoseconds deadline)
    {
        timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(deadline));
        timer.async_wait(
                [this, node_name, corr_id](std::error_code ec)
                {
                    if(ec)
                        return;
                    fire_timeout(node_name, corr_id);
                });
    }

    // A no-op if the entry already resolved, so the expiry is idempotent against a cancel race.
    void fire_timeout(const std::string &node_name, std::uint64_t corr_id)
    {
        auto peer_it = m_outstanding.find(node_name);
        if(peer_it == m_outstanding.end())
            return;
        auto entry_it = peer_it->second.find(corr_id);
        if(entry_it == peer_it->second.end())
            return;
        pending_rpc pending = std::move(entry_it->second);
        peer_it->second.erase(entry_it);
        pending.on_response(wire::rpc_status::timeout, {});
    }

    wire::bidirectional_header request_header(std::string_view fqn, std::uint64_t corr_id)
    {
        return wire::bidirectional_header{.source         = wire::endpoint_source_type::caller,
                                          .sequence       = m_endpoint.next_sequence(),
                                          .topic_hash     = wire::fqn_topic_hash(fqn),
                                          .type_hash_1    = 0,
                                          .type_hash_2    = 0,
                                          .correlation_id = corr_id};
    }

    // Frame a header-on rpc inner frame into m_frame_scratch (session_id 0 — the destination's staleness
    // gate admits it regardless of its latched epoch, mirroring the forwarded data leg).
    void stage_rpc_frame(wire::msg_type type, std::span<const std::byte> inner)
    {
        wire::frame_header fhdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = inner.size()};
        wire::encode_frame_into(m_frame_scratch, fhdr, inner);
    }

    // Build the reused reply once: it closes over the staged m_active_* context, so a steady-state
    // dispatch reuses it with no per-dispatch type-erased allocation.
    void ensure_reply_ready()
    {
        if(m_reply)
            return;
        m_reply = [this](wire::rpc_status status, std::span<const std::byte> return_data) { emit_reply(status, return_data); };
    }

    // A reply to a forwarded request routes back by the requester origin (re-resolved via route_select at
    // each hop); a reply to a direct request rides straight back on the arrival channel.
    void emit_reply(wire::rpc_status status, std::span<const std::byte> return_data)
    {
        if(m_active_reply_forwarded && m_forward_rpc)
            return emit_forwarded_reply(status, return_data);
        detail::reply_status(*this, *m_active_channel, m_active_req_hdr, status, return_data, m_active_session_id);
    }

    // Echo the correlation_id and address the reply to the requester origin — the relay holds no
    // correlation, so the reply must carry its own destination for each hop to re-resolve it.
    void emit_forwarded_reply(wire::rpc_status status, std::span<const std::byte> return_data)
    {
        wire::bidirectional_header resp_hdr{.source         = wire::endpoint_source_type::procedure,
                                            .sequence       = m_endpoint.next_sequence(),
                                            .topic_hash     = m_active_req_hdr.topic_hash,
                                            .type_hash_1    = 0,
                                            .type_hash_2    = 0,
                                            .correlation_id = m_active_req_hdr.correlation_id};
        wire::encode_rpc_response_into(m_resp_scratch, resp_hdr, status, return_data);
        stage_rpc_frame(wire::msg_type::rpc_response, m_resp_scratch);
        m_forward_rpc(m_active_reply_origin, m_frame_scratch);
    }

    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    void drop(std::string_view message)
    {
        m_logger.warn(message);
    }
};

}

#endif

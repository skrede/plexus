#ifndef HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/null_logger.h"
#include "plexus/policy.h"
#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/io/detail/procedure_fanout.h"

#include "plexus/detail/compat.h"

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

// The req/res sibling of message_forwarder: a header-only Policy-templated
// forwarder that correlates opaque-byte requests to responses. A caller call()
// allocates a monotonic correlation_id, frames a frame_header-wrapped rpc_request,
// and registers a pending entry in a bounded per-peer outstanding-request table
// (reserved at construction — an over-capacity call fails fast). A provider
// serve()s an fqn; an arriving rpc_request dispatches to its async-reply handler
// over opaque param bytes, and the handler's reply frames an rpc_response with the
// SAME correlation_id. The caller matches a response back to its pending entry by
// correlation_id; a miss is warn-and-dropped as an orphan.
//
// Parity-strict elsewhere, with ONE additive guarantee the source lacks: a
// per-call timeout. The source resolves an outstanding request only on a matching
// response or on peer-death (detach_all -> peer_disconnected); a provider that
// silently never replies hangs forever. plexus arms a Policy timer per call (a
// forwarder-level default deadline set at construction, optionally overridden
// per call): on expiry the entry fires rpc_status::timeout and is erased; a
// matched response or detach_all cancels the timer first. There is still NO
// caller cancel — an in-flight call resolves on response, timeout, or peer-death.
//
// The router-owns-demux split: this forwarder exposes
// deliver_request/deliver_response and owns NO frame_header.type switch — the
// frame_router owns the type switch and hands each the inner header-off payload.
// The wire_forwarder maintainability gate is asserted in the oracle over a
// concrete Policy (the class is templated), mirroring message_forwarder.
//
// over-limit: one cohesive req/res correlation engine; splitting scatters the per-peer outstanding
// table + the reused scratch/active-request state (the rpc fan + send-frame helpers already
// extracted to detail/).
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

    // The provider's async reply: invoke reply(status, return_bytes) to frame an
    // rpc_response carrying the request's correlation_id. Passed to a handler by
    // reference to a forwarder-owned, reused callable — so a steady-state dispatch
    // constructs no new type-erased object (the no-hot-path-allocation invariant).
    using reply_fn =
            plexus::detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // A provider handler over opaque param bytes; it replies via the reply&.
    using handler_fn =
            plexus::detail::move_only_function<void(std::span<const std::byte> param, reply_fn &)>;

    // The caller's response callback: fired once with the matched response's status
    // and opaque return bytes (or peer_disconnected/no_handler on a failure leg).
    using on_response_fn =
            plexus::detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // Default bounded outstanding capacity per peer. A plexus determinism posture
    // (no hot-path growth), not a wire change — an over-capacity call fails fast.
    static constexpr std::size_t k_default_max_outstanding = 1024;

    // The executor (to construct a per-call Policy timer), the default call deadline,
    // and the logger are REQUIRED — a forwarder cannot arm a timeout it was not told
    // to, and the logger is a borrowed engine dependency, not a defaulted convenience.
    // The deadline and logger are passed in, not defaulted to a magic constant or a
    // shared sink, per plexus's required-over-default posture: the right value depends
    // on the deployment. max_outstanding keeps its capacity default.
    procedure_forwarder(executor_type executor, std::chrono::nanoseconds default_deadline,
                        log::logger &logger,
                        std::size_t  max_outstanding = k_default_max_outstanding)
            : m_logger(logger)
            , m_executor(executor)
            , m_default_deadline(default_deadline)
            , m_max_outstanding(max_outstanding)
    {
    }

    // The RPC observation sinks, wired by the engine to its posted fan-out (the drop_sink
    // precedent): call() fires the call sink, deliver_request() the serve sink, and
    // deliver_response() the reply sink — each carrying the correlation_id (and status for a
    // reply). Each sink posts a snapshot to the observer fan-out, never inline from the
    // per-call/per-dispatch site. The param/return bytes are transient (caller-owned or
    // reused scratch), so the surfaced view is envelope-only — recording the bytes is a
    // sovereign opt-in, never a silent per-call copy into the posted turn. Absent = one
    // predictable branch.
    void
    on_rpc_call(plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> hook)
    {
        m_on_rpc_call = std::move(hook);
    }

    void
    on_rpc_serve(plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> hook)
    {
        m_on_rpc_serve = std::move(hook);
    }

    void on_rpc_reply(
            plexus::detail::move_only_function<void(std::string_view, const rpc_reply_view &)> hook)
    {
        m_on_rpc_reply = std::move(hook);
    }

    // serve: register a LOCAL provider handler and record the topic_hash -> fqn
    // resolution deliver_request reads. Emits no wire.
    void serve(std::string_view fqn, handler_fn handler)
    {
        m_hash_to_fqn[wire::fqn_topic_hash(fqn)] = std::string{fqn};
        m_providers[std::string{fqn}]            = std::move(handler);
    }

    // retire: drop a LOCAL provider handler so a subsequent inbound rpc_request for the
    // fqn resolves the existing absent-handler path (rpc_status::no_handler). The inverse
    // of serve; serve()'s direct-engine semantics (a re-serve overwrites in place) are
    // unchanged for callers that never retire.
    void retire(std::string_view fqn)
    {
        m_hash_to_fqn.erase(wire::fqn_topic_hash(fqn));
        m_providers.erase(std::string{fqn});
    }

    // call: fail fast if the peer's outstanding map is full (no insertion), else
    // allocate a corr_id, frame a frame_header-wrapped rpc_request into reused
    // scratch, send it, and ONLY THEN register the pending entry (the source
    // registers post-admission so a rejected send leaves no dangling entry). The type
    // tag words are reserved zeroes: correlation is carried by correlation_id, and type
    // matching is settled at subscribe-time discovery, so they are written 0.
    void call(const peer &p, std::string_view fqn, std::span<const std::byte> param,
              on_response_fn                          on_response,
              std::optional<std::chrono::nanoseconds> deadline   = std::nullopt,
              std::uint64_t                           session_id = 0)
    {
        auto &per_peer = m_outstanding[p.node_name];
        if(per_peer.size() >= m_max_outstanding)
            return on_response(wire::rpc_status::error, {});

        std::uint64_t              corr_id = m_next_correlation_id++;
        wire::bidirectional_header hdr{.source         = wire::endpoint_source_type::caller,
                                       .sequence       = m_endpoint.next_sequence(),
                                       .topic_hash     = wire::fqn_topic_hash(fqn),
                                       .type_hash_1    = 0,
                                       .type_hash_2    = 0,
                                       .correlation_id = corr_id};
        wire::encode_rpc_request_into(m_req_scratch, hdr, param);
        detail::send_data(*this, p.channel, wire::msg_type::rpc_request, m_req_scratch, session_id);
        detail::emit_rpc_call(*this, fqn, corr_id);

        auto [it, _] = per_peer.emplace(corr_id,
                                        pending_rpc{std::string{fqn}, std::move(on_response),
                                                    std::make_unique<timer_type>(m_executor)});
        arm_deadline(p.node_name, corr_id, *it->second.timer,
                     deadline.value_or(m_default_deadline));
    }

    // attach: per-(peer, fqn) refcount gate. On 0->1 it emits a procedure subscribe
    // and returns true. plexus has no remote registry, so attach always succeeds on
    // the 0->1 transition — a divergence from the source, which returns false on an
    // unknown remote topic. A call() needs no prior attach (point-to-point).
    bool attach(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name);
        detail::send_subscribe(*this, p.channel, fqn, hash);
        return true;
    }

    // attach_for_fanout: same gate + entry, but emits a subscribe_response (the
    // provider's reaction to an arriving subscribe).
    bool attach_for_fanout(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name);
        auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = wire::subscribe_status::subscribed});
        send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return true;
    }

    // detach: per-(peer, fqn) refcount gate. On 1->0 it emits an unsubscribe.
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

    // detach_all: drop the peer's subscriber entries/refcounts, then fire EVERY
    // pending callback for the peer with peer_disconnected and erase the peer's
    // outstanding map. Cancels each entry's deadline timer first so a torn-down
    // entry never fires a late timeout. The peer-death resolution path.
    void detach_all(const peer &p)
    {
        m_endpoint.remove_peer(p);

        auto peer_it = m_outstanding.find(p.node_name);
        if(peer_it == m_outstanding.end())
            return;
        for(auto &[corr_id, pending] : peer_it->second)
        {
            pending.timer->cancel();
            pending.on_response(wire::rpc_status::peer_disconnected, {});
        }
        m_outstanding.erase(peer_it);
    }

    // deliver_request (provider receive tail): decode the inbound (header-off)
    // rpc_request, resolve a local provider by topic_hash, and dispatch to it over
    // the opaque param bytes. A request whose topic_hash has no served provider
    // resolves the caller's pending entry with no_handler (never silent, never
    // left hanging) UNLESS the subscriber registry knows the fqn for this hash from
    // a prior attach but no provider is served — that "topic known, provider gone"
    // state replies topic_not_found. The handler replies via a reply_fn that frames
    // a same-corr_id rpc_response (source = procedure, swapped type hashes).
    void deliver_request(const peer &p, std::span<const std::byte> inner,
                         std::uint64_t session_id = 0)
    {
        auto decoded = wire::decode_rpc_request(inner);
        if(!decoded)
            return drop("plexus: forwarder rpc_request decode_failed");

        const auto req_hdr = decoded->header;
        auto       hash_it = m_hash_to_fqn.find(req_hdr.topic_hash);
        if(hash_it == m_hash_to_fqn.end())
        {
            auto status = m_endpoint.registry().fqn_for(req_hdr.topic_hash).empty()
                    ? wire::rpc_status::no_handler
                    : wire::rpc_status::topic_not_found;
            return detail::reply_status(*this, p.channel, req_hdr, status, {}, session_id);
        }

        // Stage the active request context, then hand the handler the forwarder's
        // reused reply (constructed once, on first dispatch) by reference — no
        // per-dispatch type-erased object is built.
        m_active_channel    = &p.channel;
        m_active_req_hdr    = req_hdr;
        m_active_session_id = session_id;
        ensure_reply_ready();
        detail::emit_rpc_serve(*this, hash_it->second, req_hdr.correlation_id);
        m_providers.find(hash_it->second)->second(decoded->param_data, m_reply);
    }

    // deliver_response (caller receive tail): decode the inbound (header-off)
    // rpc_response, find the pending entry for this peer by correlation_id, move it
    // out + erase it, and fire its callback. A non-matching corr_id is warn-dropped
    // as an orphan — never fires a callback (corr_id uniqueness ⇒ ≤1 match).
    void deliver_response(const peer &p, std::span<const std::byte> inner)
    {
        auto decoded = wire::decode_rpc_response(inner);
        if(!decoded)
            return drop("plexus: forwarder rpc_response decode_failed");

        auto peer_it = m_outstanding.find(p.node_name);
        if(peer_it == m_outstanding.end())
            return drop("plexus: forwarder rpc_response_orphan");

        auto entry_it = peer_it->second.find(decoded->header.correlation_id);
        if(entry_it == peer_it->second.end())
            return drop("plexus: forwarder rpc_response_orphan");

        pending_rpc pending = std::move(entry_it->second);
        peer_it->second.erase(entry_it);
        pending.timer->cancel(); // cancel-on-match: no late timeout for a resolved call
        detail::emit_rpc_reply(*this, pending.fqn, decoded->header.correlation_id, decoded->status);
        pending.on_response(decoded->status, decoded->return_data);
    }

private:
    template<typename F>
    friend void detail::emit_rpc_call(F &, std::string_view, std::uint64_t);
    template<typename F>
    friend void detail::emit_rpc_serve(F &, std::string_view, std::uint64_t);
    template<typename F>
    friend void detail::emit_rpc_reply(F &, std::string_view, std::uint64_t, wire::rpc_status);
    template<typename F, typename C>
    friend void detail::reply_status(F &, C &, const wire::bidirectional_header &, wire::rpc_status,
                                     std::span<const std::byte>, std::uint64_t);
    template<typename F, typename C>
    friend void detail::send_data(F &, C &, wire::msg_type, std::span<const std::byte>,
                                  std::uint64_t);
    template<typename F, typename C>
    friend void detail::send_subscribe(F &, C &, std::string_view, std::uint64_t);

    // A registered outstanding request. Owns its deadline timer (one heap node per
    // outstanding call, consistent with the bounded per-call map cost — NOT a
    // dispatch-path allocation) so the entry stays movable on a matched response.
    struct pending_rpc
    {
        std::string                 fqn;
        on_response_fn              on_response;
        std::unique_ptr<timer_type> timer;
    };

    // arm_deadline: start the per-call timer. On a clean expiry (the response never
    // arrived) fire rpc_status::timeout and erase the entry; a cancellation (match
    // or detach_all) lands as operation_canceled and is a no-op. The handler looks
    // the entry up by (node_name, corr_id) — never captures the entry, which moves.
    void arm_deadline(const std::string &node_name, std::uint64_t corr_id, timer_type &timer,
                      std::chrono::nanoseconds deadline)
    {
        timer.expires_after(std::chrono::duration_cast<std::chrono::milliseconds>(deadline));
        timer.async_wait(
                [this, node_name, corr_id](std::error_code ec)
                {
                    if(ec)
                        return; // cancelled by a matched response or detach_all
                    fire_timeout(node_name, corr_id);
                });
    }

    // fire_timeout: resolve a still-outstanding entry with rpc_status::timeout and
    // erase it. A no-op if the entry already resolved (a race the cancel forecloses,
    // but the lookup makes the expiry idempotent regardless).
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

    // ensure_reply_ready: construct the forwarder's reused reply once (lazily). It
    // closes over `this` and reads the staged m_active_* request context, so a
    // steady-state dispatch reuses it with no per-dispatch type-erased allocation.
    void ensure_reply_ready()
    {
        if(m_reply)
            return;
        m_reply = [this](wire::rpc_status status, std::span<const std::byte> return_data)
        {
            detail::reply_status(*this, *m_active_channel, m_active_req_hdr, status, return_data,
                                 m_active_session_id);
        };
    }

    // send_control: wrap an inner CONTROL payload (subscribe / unsubscribe /
    // subscribe_response) in a frame_header with session_id = 0 (control is
    // pre-session) into reused scratch and send it.
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger                                   &m_logger;
    endpoint_type                                  m_endpoint;
    executor_type                                  m_executor;
    std::chrono::nanoseconds                       m_default_deadline;
    std::unordered_map<std::string, handler_fn>    m_providers;
    std::unordered_map<std::uint64_t, std::string> m_hash_to_fqn;
    std::unordered_map<std::string, std::unordered_map<std::uint64_t, pending_rpc>> m_outstanding;
    std::vector<std::byte>                                                          m_req_scratch;
    std::vector<std::byte>                                                          m_resp_scratch;
    std::vector<std::byte>                                                          m_frame_scratch;
    reply_fn                                                                        m_reply;
    channel_type              *m_active_channel{nullptr};
    wire::bidirectional_header m_active_req_hdr{};
    std::uint64_t              m_active_session_id{0};
    std::uint64_t              m_next_correlation_id{1};
    std::size_t                m_max_outstanding;
    plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> m_on_rpc_call;
    plexus::detail::move_only_function<void(std::string_view, const rpc_view &)> m_on_rpc_serve;
    plexus::detail::move_only_function<void(std::string_view, const rpc_reply_view &)>
            m_on_rpc_reply;
};

}

#endif

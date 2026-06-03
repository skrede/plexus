#ifndef HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_PROCEDURE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
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

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The req/res sibling of message_forwarder: a header-only Policy-templated
// forwarder that correlates opaque-byte requests to responses. A caller call()
// allocates a monotonic correlation_id, frames a frame_header-wrapped rpc_request,
// and registers a pending entry in a bounded per-peer outstanding-request table
// (reserved at construction — an over-capacity call fails fast). A
// provider serve()s an fqn; an arriving rpc_request dispatches to its async-reply
// handler over opaque param bytes, and the handler's reply frames an rpc_response
// with the SAME correlation_id. The caller matches a response back to its
// pending entry by correlation_id; a miss is warn-and-dropped as an orphan.
//
// Parity-strict (mirrors the source): there is NO per-call timeout (no Policy
// timer armed, no per-call expiry) and NO caller cancel. An outstanding request
// resolves ONLY on a matching response or on peer-death
// (detach_all -> peer_disconnected).
//
// The router-owns-demux split: this forwarder exposes
// deliver_request/deliver_response and owns NO frame_header.type switch — the
// frame_router owns the type switch and hands each the inner header-off payload.
// The wire_forwarder maintainability gate is asserted in the oracle over a
// concrete Policy (the class is templated), mirroring message_forwarder.
template <typename Policy>
    requires plexus::Policy<Policy>
class procedure_forwarder
{
public:
    using channel_type = typename Policy::byte_channel_type;

    // A peer the forwarder talks to: the channel a frame rides plus the node-name
    // key its outstanding table is rooted at (mirror message_forwarder).
    struct peer
    {
        channel_type &channel;
        std::string node_name;
    };

    // The provider's async reply: invoke reply_fn(status, return_bytes) to
    // frame an rpc_response carrying the request's correlation_id.
    using reply_fn = detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // A provider handler over opaque param bytes; it replies via the reply_fn.
    using handler_fn = detail::move_only_function<void(std::span<const std::byte> param, reply_fn)>;

    // The caller's response callback: fired once with the matched response's status
    // and opaque return bytes (or peer_disconnected/no_handler on a failure leg).
    using on_response_fn = detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // Default bounded outstanding capacity reserved per peer at first use. A plexus
    // determinism posture (no hot-path alloc), not a wire change — an over-capacity
    // call fails fast with rpc_status::error.
    static constexpr std::size_t k_default_max_outstanding = 1024;

    explicit procedure_forwarder(log::logger &logger = shared_null_logger(),
                                 std::size_t max_outstanding = k_default_max_outstanding) noexcept
        : m_logger(logger)
        , m_max_outstanding(max_outstanding)
    {
    }

    // serve: register a LOCAL provider handler for an fqn and record the
    // topic_hash -> fqn resolution deliver_request reads. Emits NO wire.
    void serve(std::string_view fqn, handler_fn handler);

    // call: allocate a corr_id, frame+send an rpc_request, then register the
    // pending entry (after the send is accepted). An over-capacity call fails fast
    // via on_response(rpc_status::error, {}) and inserts nothing.
    void call(const peer &p, std::string_view fqn, std::span<const std::byte> param, on_response_fn on_response);

    // attach: per-(peer, fqn) refcount gate. On 0->1 it emits a procedure
    // subscribe_request and returns true (a call needs no prior attach).
    bool attach(const peer &p, std::string_view fqn);

    // attach_for_fanout: same gate, but emits a subscribe_response (the provider's
    // reaction to an arriving subscribe).
    bool attach_for_fanout(const peer &p, std::string_view fqn);

    // detach: per-(peer, fqn) refcount gate. On 1->0 it emits an unsubscribe.
    bool detach(const peer &p, std::string_view fqn);

    // detach_all: drop the peer's subscriber state, then fire every outstanding
    // callback for that peer with peer_disconnected and clear them. The ONLY
    // non-response resolution path (no timer).
    void detach_all(const peer &p);

    // drain_for: re-emit a subscribe for each remote topic rooted at the peer.
    void drain_for(const peer &p);

    // The provider receive tail: dispatch an inbound (header-off) rpc_request to its
    // handler and frame the same-corr_id rpc_response reply.
    void deliver_request(const peer &p, std::span<const std::byte> inner);

    // The caller receive tail: match an inbound (header-off) rpc_response back to a
    // pending entry by correlation_id, fire its callback, and erase it.
    void deliver_response(const peer &p, std::span<const std::byte> inner);

private:
    // A registered outstanding request. NO per-call expiry field: an
    // entry resolves only on a matching response or peer-death.
    struct pending_rpc
    {
        std::string fqn;
        on_response_fn on_response;
    };

    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner);
    void send_subscribe(channel_type &channel, std::string_view fqn, std::uint64_t hash);
    void record_remote_topic(const std::string &node_name, std::string_view fqn);
    void reply_status(const peer &p, const wire::bidirectional_header &req_hdr, wire::rpc_status status,
                      std::span<const std::byte> return_data);
    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger &m_logger;
    subscriber_registry<channel_type> m_registry;
    std::unordered_map<std::string, handler_fn> m_providers;
    std::unordered_map<std::uint64_t, std::string> m_hash_to_fqn;
    std::unordered_map<std::string, std::unordered_map<std::uint64_t, pending_rpc>> m_outstanding;
    std::unordered_map<std::string, std::vector<std::string>> m_remote_topics;
    std::vector<std::byte> m_req_scratch;
    std::vector<std::byte> m_resp_scratch;
    std::vector<std::byte> m_frame_scratch;
    std::vector<std::byte> m_control_scratch;
    std::uint64_t m_next_correlation_id{1};
    std::uint64_t m_next_sequence{0};
    std::size_t m_max_outstanding;
};

// --- Caller half: correlation, response-match, peer-death resolution ---

// call: fail fast if the peer's outstanding map is full (no insertion), else
// allocate a corr_id, frame a frame_header-wrapped rpc_request into reused
// scratch, send it, and ONLY THEN register the pending entry (the source
// registers post-admission so a rejected send leaves no dangling entry). Type
// hashes are opaque correlation hints plexus never interprets — written 0.
template <typename Policy>
    requires plexus::Policy<Policy>
void procedure_forwarder<Policy>::call(const peer &p, std::string_view fqn,
                                       std::span<const std::byte> param, on_response_fn on_response)
{
    auto &per_peer = m_outstanding[p.node_name];
    if(per_peer.size() >= m_max_outstanding)
        return on_response(wire::rpc_status::error, {});

    std::uint64_t corr_id = m_next_correlation_id++;
    wire::bidirectional_header hdr{
            .source         = wire::endpoint_source_type::caller,
            .sequence       = m_next_sequence++,
            .topic_hash     = wire::fqn_topic_hash(fqn),
            .type_hash_1    = 0,
            .type_hash_2    = 0,
            .correlation_id = corr_id
    };
    wire::encode_rpc_request_into(m_req_scratch, hdr, param);
    send_control(p.channel, wire::msg_type::rpc_request, m_req_scratch);

    per_peer.emplace(corr_id, pending_rpc{std::string{fqn}, std::move(on_response)});
}

// deliver_response: decode the inbound (header-off) rpc_response, find the
// pending entry for this peer by correlation_id, move it out + erase it, and fire
// its callback. A non-matching corr_id is warn-and-dropped as an orphan — never
// fires a callback (corr_id uniqueness guarantees <= 1 match).
template <typename Policy>
    requires plexus::Policy<Policy>
void procedure_forwarder<Policy>::deliver_response(const peer &p, std::span<const std::byte> inner)
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
    pending.on_response(decoded->status, decoded->return_data);
}

// detach_all: drop the peer's subscriber entries/refcounts, then fire EVERY
// pending callback for the peer with peer_disconnected (empty return bytes) and
// erase the peer's outstanding map. The ONLY non-response resolution path.
template <typename Policy>
    requires plexus::Policy<Policy>
void procedure_forwarder<Policy>::detach_all(const peer &p)
{
    m_registry.remove_peer(p.node_name, p.channel);
    m_remote_topics.erase(p.node_name);

    auto peer_it = m_outstanding.find(p.node_name);
    if(peer_it == m_outstanding.end())
        return;
    for(auto &[corr_id, pending] : peer_it->second)
        pending.on_response(wire::rpc_status::peer_disconnected, {});
    m_outstanding.erase(peer_it);
}

// send_control: wrap an inner payload in a frame_header (session_id = 0, every
// frame uniformly framed) into reused scratch and send it. Shared by the caller
// request emit, the provider response emit, and the control emits.
template <typename Policy>
    requires plexus::Policy<Policy>
void procedure_forwarder<Policy>::send_control(channel_type &channel, wire::msg_type type,
                                               std::span<const std::byte> inner)
{
    wire::frame_header fhdr{
            .type         = type,
            .flags        = 0,
            .session_id   = 0,
            .timestamp_ns = wire::now_timestamp_ns(),
            .payload_len  = inner.size()
    };
    wire::encode_frame_into(m_frame_scratch, fhdr, inner);
    channel.send(m_frame_scratch);
}

}

#endif

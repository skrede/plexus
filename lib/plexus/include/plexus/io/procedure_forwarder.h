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
// (reserved at construction — an over-capacity call fails fast). A provider
// serve()s an fqn; an arriving rpc_request dispatches to its async-reply handler
// over opaque param bytes, and the handler's reply frames an rpc_response with the
// SAME correlation_id. The caller matches a response back to its pending entry by
// correlation_id; a miss is warn-and-dropped as an orphan.
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

    // The provider's async reply: invoke reply_fn(status, return_bytes) to frame an
    // rpc_response carrying the request's correlation_id.
    using reply_fn = detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // A provider handler over opaque param bytes; it replies via the reply_fn.
    using handler_fn = detail::move_only_function<void(std::span<const std::byte> param, reply_fn)>;

    // The caller's response callback: fired once with the matched response's status
    // and opaque return bytes (or peer_disconnected/no_handler on a failure leg).
    using on_response_fn = detail::move_only_function<void(wire::rpc_status, std::span<const std::byte>)>;

    // Default bounded outstanding capacity per peer. A plexus determinism posture
    // (no hot-path growth), not a wire change — an over-capacity call fails fast.
    static constexpr std::size_t k_default_max_outstanding = 1024;

    explicit procedure_forwarder(log::logger &logger = shared_null_logger(),
                                 std::size_t max_outstanding = k_default_max_outstanding) noexcept
        : m_logger(logger)
        , m_max_outstanding(max_outstanding)
    {
    }

    // serve: register a LOCAL provider handler and record the topic_hash -> fqn
    // resolution deliver_request reads. Emits no wire.
    void serve(std::string_view fqn, handler_fn handler)
    {
        m_hash_to_fqn[wire::fqn_topic_hash(fqn)] = std::string{fqn};
        m_providers[std::string{fqn}] = std::move(handler);
    }

    // call: fail fast if the peer's outstanding map is full (no insertion), else
    // allocate a corr_id, frame a frame_header-wrapped rpc_request into reused
    // scratch, send it, and ONLY THEN register the pending entry (the source
    // registers post-admission so a rejected send leaves no dangling entry). Type
    // hashes are opaque correlation hints plexus never interprets — written 0.
    void call(const peer &p, std::string_view fqn, std::span<const std::byte> param, on_response_fn on_response)
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

    // attach: per-(peer, fqn) refcount gate. On 0->1 it emits a procedure subscribe
    // and returns true. plexus has no remote registry, so attach always succeeds on
    // the 0->1 transition — a divergence from the source, which returns false on an
    // unknown remote topic. A call() needs no prior attach (point-to-point).
    bool attach(const peer &p, std::string_view fqn)
    {
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name);
        record_remote_topic(p.node_name, fqn);
        send_subscribe(p.channel, fqn, hash);
        return true;
    }

    // attach_for_fanout: same gate + entry, but emits a subscribe_response (the
    // provider's reaction to an arriving subscribe).
    bool attach_for_fanout(const peer &p, std::string_view fqn)
    {
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name);
        auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = wire::subscribe_status::subscribed});
        send_control(p.channel, wire::msg_type::subscribe_response, resp);
        return true;
    }

    // detach: per-(peer, fqn) refcount gate. On 1->0 it emits an unsubscribe.
    bool detach(const peer &p, std::string_view fqn)
    {
        if(m_registry.drop_refcount(p.node_name, fqn) != 0u)
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.remove_subscriber(hash, p.channel);
        auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
        send_control(p.channel, wire::msg_type::unsubscribe, req);
        return true;
    }

    // detach_all: drop the peer's subscriber entries/refcounts, then fire EVERY
    // pending callback for the peer with peer_disconnected and erase the peer's
    // outstanding map. The ONLY non-response resolution path (no timer).
    void detach_all(const peer &p)
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

    // drain_for: re-emit a subscribe for each remote topic rooted at the peer's
    // node name (subscription resurrection on reconnect).
    void drain_for(const peer &p)
    {
        auto it = m_remote_topics.find(p.node_name);
        if(it == m_remote_topics.end())
            return;
        for(const auto &fqn : it->second)
            send_subscribe(p.channel, fqn, wire::fqn_topic_hash(fqn));
    }

    // deliver_request (provider receive tail): decode the inbound (header-off)
    // rpc_request, resolve its fqn by topic_hash, and dispatch to the registered
    // handler over the opaque param bytes. An unknown topic replies topic_not_found;
    // a known fqn with no provider replies no_handler — neither leaves the caller
    // hanging. The handler replies via a reply_fn that frames a same-corr_id
    // rpc_response (source = procedure, swapped type hashes).
    void deliver_request(const peer &p, std::span<const std::byte> inner)
    {
        auto decoded = wire::decode_rpc_request(inner);
        if(!decoded)
            return drop("plexus: forwarder rpc_request decode_failed");

        const auto req_hdr = decoded->header;
        auto hash_it = m_hash_to_fqn.find(req_hdr.topic_hash);
        if(hash_it == m_hash_to_fqn.end())
            return reply_status(p, req_hdr, wire::rpc_status::topic_not_found, {});

        auto provider_it = m_providers.find(hash_it->second);
        if(provider_it == m_providers.end())
            return reply_status(p, req_hdr, wire::rpc_status::no_handler, {});

        auto reply = [this, &p, req_hdr](wire::rpc_status status, std::span<const std::byte> return_data) {
            reply_status(p, req_hdr, status, return_data);
        };
        provider_it->second(decoded->param_data, std::move(reply));
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
        pending.on_response(decoded->status, decoded->return_data);
    }

private:
    // A registered outstanding request. NO per-call expiry field: an entry resolves
    // only on a matching response or peer-death.
    struct pending_rpc
    {
        std::string fqn;
        on_response_fn on_response;
    };

    // reply_status: frame an rpc_response carrying the request's correlation_id back
    // to the peer (source = procedure, type hashes swapped relative to the request)
    // and send it through reused scratch.
    void reply_status(const peer &p, const wire::bidirectional_header &req_hdr,
                      wire::rpc_status status, std::span<const std::byte> return_data)
    {
        wire::bidirectional_header resp_hdr{
                .source         = wire::endpoint_source_type::procedure,
                .sequence       = m_next_sequence++,
                .topic_hash     = req_hdr.topic_hash,
                .type_hash_1    = req_hdr.type_hash_2,
                .type_hash_2    = req_hdr.type_hash_1,
                .correlation_id = req_hdr.correlation_id
        };
        wire::encode_rpc_response_into(m_resp_scratch, resp_hdr, status, return_data);
        send_control(p.channel, wire::msg_type::rpc_response, m_resp_scratch);
    }

    // send_control: wrap an inner payload in a frame_header (session_id = 0, every
    // frame uniformly framed) into reused scratch and send it. Shared by the request
    // emit, the response emit, and the control emits.
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
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

    void send_subscribe(channel_type &channel, std::string_view fqn, std::uint64_t hash)
    {
        wire::subscribe_request req{
                .fqn        = std::string{fqn},
                .type_name  = {},
                .topic_hash = hash,
                .type_hash  = 0,
                .source     = wire::endpoint_source_type::caller
        };
        auto bytes = wire::encode_subscribe_request(req);
        send_control(channel, wire::msg_type::subscribe, bytes);
    }

    void record_remote_topic(const std::string &node_name, std::string_view fqn)
    {
        auto &topics = m_remote_topics[node_name];
        for(const auto &existing : topics)
            if(existing == fqn)
                return;
        topics.emplace_back(fqn);
    }

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
    std::uint64_t m_next_correlation_id{1};
    std::uint64_t m_next_sequence{0};
    std::size_t m_max_outstanding;
};

}

#endif

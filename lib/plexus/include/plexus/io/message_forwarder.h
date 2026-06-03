#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/wire_bytes.h"
#include "plexus/policy.h"
#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The shared null sink the forwarder is injected with when the caller supplies
// no logger: the warn-and-drop seam exists, but is silent. A function-local
// static (no static singleton object at namespace scope) bound by reference.
inline log::logger &shared_null_logger()
{
    static log::null_logger sink;
    return sink;
}

// Stable fqn -> topic_hash for the slice. plexus carries no hashing dependency,
// so the wire identity is computed here with 64-bit FNV-1a: deterministic for
// identical bytes on every platform (unlike std::hash, which is toolchain-
// defined), which is the only correct property for an identifier two peers must
// agree on. attach and publish both route through this one function, so the
// receive tail's resolve-by-hash is self-consistent.
inline std::uint64_t fqn_topic_hash(std::string_view fqn) noexcept
{
    std::uint64_t h = 1469598103934665603ull;
    for(char c : fqn)
    {
        h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        h *= 1099511628211ull;
    }
    return h;
}

// The slice's payload-opaque pub/sub engine: a header-only forwarder templated
// on the Policy seam that fans opaque wire_bytes over byte_channels. A "peer" is
// the byte_channel a frame rides plus the node-name key its subscription is
// rooted at. The forwarder is refcount-gated (one subscribe per uninterrupted
// attach run), frames each publish ONCE and shares the single owning buffer
// across subscribers, and warn-and-drops a malformed frame on the receive tail
// through an injected cold-path logger& (default the shared null_logger). It
// never interprets the payload — published bytes are framed as-is and received
// bytes are handed up opaque.
template <typename Policy>
    requires plexus::Policy<Policy>
class message_forwarder
{
public:
    using channel_type = typename Policy::byte_channel_type;

    // A peer the forwarder fans toward: the channel plus its node-name key. The
    // public API takes peers by const& (no raw pointers).
    struct peer
    {
        channel_type &channel;
        std::string node_name;
    };

    explicit message_forwarder(log::logger &logger = shared_null_logger()) noexcept
        : m_logger(logger)
    {
    }

    // attach: per-(peer, fqn) refcount gate. On the 0->1 transition it registers
    // the fan-out entry AND emits a wire::subscribe_request to the peer; returns
    // true. Subsequent attaches only bump the refcount and return false.
    bool attach(const peer &p, std::string_view fqn)
    {
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        auto hash = fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name);
        record_remote_topic(p.node_name, fqn);
        send_subscribe(p.channel, fqn, hash);
        return true;
    }

    // attach_for_fanout: same gate + entry, but emits a wire::subscribe_response
    // (NOT a subscribe). The producer-side reaction to an arriving subscribe.
    bool attach_for_fanout(const peer &p, std::string_view fqn)
    {
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        auto hash = fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name);
        auto resp = wire::encode_subscribe_response(
            {.topic_hash = hash, .status = wire::subscribe_status::subscribed});
        p.channel.send(resp);
        return true;
    }

    // publish: frame ONCE (unidirectional header + frame_header, session_id = 0,
    // no metadata region) and fan the single owning buffer to each subscribed
    // channel. No subscriber -> no send (demand-driven). No per-listener reframe
    // or allocation in the loop.
    void publish(std::string_view fqn, std::span<const std::byte> payload)
    {
        auto hash = fqn_topic_hash(fqn);
        const auto *subs = m_registry.subscribers_for(hash);
        if(subs == nullptr)
            return;

        wire::unidirectional_header uhdr{
                .source     = wire::endpoint_source_type::publisher,
                .sequence   = m_next_sequence++,
                .topic_hash = hash,
                .type_hash  = 0
        };
        auto inner = wire::encode_unidirectional(uhdr, payload);

        wire::frame_header fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = 0,
                .session_id   = 0,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = inner.size()
        };
        auto frame = wire::encode_frame(fhdr, inner);

        for(const auto &sub : *subs)
            sub.channel->send(frame);
    }

    // detach: per-(peer, fqn) refcount gate. On the 1->0 transition it removes
    // the fan-out entry AND emits a wire::unsubscribe_request; returns true.
    bool detach(const peer &p, std::string_view fqn)
    {
        if(m_registry.drop_refcount(p.node_name, fqn) != 0u)
            return false;
        auto hash = fqn_topic_hash(fqn);
        m_registry.remove_subscriber(hash, p.channel);
        auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
        p.channel.send(req);
        return true;
    }

    // detach_all: drop all the peer's fan-out entries and refcounts; NO wire emit
    // (the peer is gone, the wire has already torn down).
    void detach_all(const peer &p)
    {
        m_registry.remove_peer(p.node_name, p.channel);
        m_remote_topics.erase(p.node_name);
    }

    // drain_for: re-emit a wire::subscribe for each remote topic rooted at the
    // peer's node name (subscription resurrection on reconnect).
    void drain_for(const peer &p)
    {
        auto it = m_remote_topics.find(p.node_name);
        if(it == m_remote_topics.end())
            return;
        for(const auto &fqn : it->second)
            send_subscribe(p.channel, fqn, fqn_topic_hash(fqn));
    }

    // The receive tail: given a reassembled frame, strip the frame_header,
    // decode the unidirectional payload, resolve the fqn by topic_hash, and hand
    // the opaque wire_bytes up to on_message (plexus never parses them). A
    // decode/verify failure — a bad header, a non-unidirectional type, a
    // malformed inner payload, or an unresolved topic_hash — is warn-and-DROPPED
    // through the injected logger&: never thrown, never propagated, never crashed.
    template <typename OnMessage>
    void deliver(std::span<const std::byte> frame, OnMessage &&on_message)
    {
        auto fhdr = wire::decode_header(frame);
        if(!fhdr || fhdr->type != wire::msg_type::unidirectional)
            return drop("plexus: forwarder frame_decode_failed");

        auto inner = frame.subspan(wire::header_size);
        auto decoded = wire::decode_unidirectional(inner);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_registry.fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        on_message(fqn, decoded->data);
    }

private:
    void send_subscribe(channel_type &channel, std::string_view fqn, std::uint64_t hash)
    {
        wire::subscribe_request req{
                .fqn        = std::string{fqn},
                .type_name  = {},
                .topic_hash = hash,
                .type_hash  = 0,
                .source     = wire::endpoint_source_type::publisher
        };
        auto bytes = wire::encode_subscribe_request(req);
        channel.send(bytes);
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
    std::unordered_map<std::string, std::vector<std::string>> m_remote_topics;
    std::uint64_t m_next_sequence{0};
};

}

#endif

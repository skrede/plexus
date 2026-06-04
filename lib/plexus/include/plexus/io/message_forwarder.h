#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/io/null_logger.h"
#include "plexus/wire_bytes.h"
#include "plexus/topic_qos.h"
#include "plexus/policy.h"
#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
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
        auto hash = wire::fqn_topic_hash(fqn);
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
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name);
        auto resp = wire::encode_subscribe_response(
            {.topic_hash = hash, .status = wire::subscribe_status::subscribed});
        send_control(p.channel, wire::msg_type::subscribe_response, resp);
        replay_if_latched(p, hash);
        return true;
    }

    // declare: mark a topic with a publisher-declared qos once (Fork-A). A latched
    // topic retains its last published frame and replays it to late subscribers.
    // The hot publish(fqn, bytes) signature is unchanged.
    void declare(std::string_view fqn, topic_qos qos)
    {
        m_registry.declare(wire::fqn_topic_hash(fqn), fqn, qos);
    }

    // latch: convenience for declare(fqn, {.latch = true, .depth = 1}).
    void latch(std::string_view fqn)
    {
        declare(fqn, topic_qos{.latch = true, .depth = 1});
    }

    // publish: frame ONCE (unidirectional header + frame_header, no metadata region)
    // and fan the single owning buffer to each subscribed channel. No subscriber ->
    // no send (demand-driven). No per-listener reframe or allocation in the loop. The
    // per-send session_id stamps the established epoch onto the data frame so the
    // receive-side staleness gate can fire; absence keeps the unestablished sentinel
    // 0 — passed per send (NOT a forwarder-wide member) because a node-shared
    // forwarder fans to many peers, each with its own epoch.
    void publish(std::string_view fqn, std::span<const std::byte> payload,
                 std::uint8_t session_id = 0)
    {
        auto hash = wire::fqn_topic_hash(fqn);
        const auto *subs = m_registry.subscribers_for(hash);
        const bool latched = m_registry.qos_for(hash).latch;
        if(subs == nullptr && !latched)
            return;   // neither a subscriber nor a latch reason to frame

        wire::unidirectional_header uhdr{
                .source     = wire::endpoint_source_type::publisher,
                .sequence   = m_next_sequence++,
                .topic_hash = hash,
                .type_hash  = 0
        };
        // Frame ONCE into the reused scratch buffers: after the first publish
        // grows them, resize() reuses capacity so steady-state publishes do not
        // allocate (the SLICE-3 no-hot-path-allocation property, designed in here).
        wire::encode_unidirectional_into(m_inner_scratch, uhdr, payload);

        wire::frame_header fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = 0,
                .session_id   = session_id,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = m_inner_scratch.size()
        };
        wire::encode_frame_into(m_frame_scratch, fhdr, m_inner_scratch);

        if(subs != nullptr)
            for(const auto &sub : *subs)
                sub.channel->send(m_frame_scratch);

        retain_if_latched(hash);
    }

    // detach: per-(peer, fqn) refcount gate. On the 1->0 transition it removes
    // the fan-out entry AND emits a wire::unsubscribe_request; returns true.
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
            send_subscribe(p.channel, fqn, wire::fqn_topic_hash(fqn));
    }

    // The receive tail: given the source peer and the INNER unidirectional payload
    // (header-OFF — the frame_router owns the frame_header strip and the type switch,
    // per the router-owns-demux split), decode it, resolve the fqn by topic_hash, and
    // hand the opaque wire_bytes up to on_message (plexus never parses them). The peer
    // leads for receive-path identity symmetry with the procedure side (delivery still
    // resolves by topic_hash, not by peer). A
    // decode/verify failure — a malformed inner payload or an unresolved
    // topic_hash — is warn-and-DROPPED through the injected logger&: never thrown,
    // never propagated, never crashed.
    template <typename OnMessage>
    void deliver(const peer &p, std::span<const std::byte> inner, OnMessage &&on_message)
    {
        (void)p;   // identity symmetry with the procedure receive tail; resolution is by topic_hash
        auto decoded = wire::decode_unidirectional(inner);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_registry.fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        on_message(fqn, decoded->data);
    }

private:
    // Wrap an inner control payload in a frame_header (Seed 3 — every control
    // frame carries the same framing as data so it survives a real reassembler-
    // framed stream) and send it. session_id = 0 on every control frame (Seed 1
    // stays deferred — no per-peer stamp). Reuses a member scratch so the control
    // emits stay allocation-light, consistent with the publish data path.
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        wire::frame_header fhdr{
                .type         = type,
                .flags        = 0,
                .session_id   = 0,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = inner.size()
        };
        wire::encode_frame_into(m_control_scratch, fhdr, inner);
        channel.send(m_control_scratch);
    }

    // retain_if_latched: on a latched publish, assign() the just-framed complete
    // frame into the per-topic retained slot. assign() reuses a slot grown by a
    // prior retain — alloc-free after warm-up (the scratch-buffer trick). depth=1:
    // one slot, replaced each latched publish — multi-publisher to one latched
    // topic is last-writer-wins per topic_hash. The slot OWNS its bytes; it never
    // aliases m_frame_scratch (the next publish overwrites that).
    void retain_if_latched(std::uint64_t hash)
    {
        if(!m_registry.qos_for(hash).latch)
            return;
        auto &slot = m_retained[hash];
        slot.assign(m_frame_scratch.begin(), m_frame_scratch.end());
    }

    // replay_if_latched: when a new subscriber attaches to a latched topic that has
    // a frame retained, PUSH that frame to ONLY the new peer (not the fan-out loop)
    // as an ordinary data frame, after the subscribe_response. A latched-but-never-
    // published topic retains nothing, so it replays nothing.
    void replay_if_latched(const peer &p, std::uint64_t hash)
    {
        if(!m_registry.qos_for(hash).latch)
            return;
        auto it = m_retained.find(hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        p.channel.send(it->second);
    }

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
    std::unordered_map<std::string, std::vector<std::string>> m_remote_topics;
    std::vector<std::byte> m_inner_scratch;
    std::vector<std::byte> m_frame_scratch;
    std::vector<std::byte> m_control_scratch;
    std::unordered_map<std::uint64_t, std::vector<std::byte>> m_retained;
    std::uint64_t m_next_sequence{0};
};

}

#endif

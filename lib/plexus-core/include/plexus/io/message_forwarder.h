#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/qos_rxo.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/null_logger.h"
#include "plexus/io/priority.h"
#include "plexus/io/locality.h"
#include "plexus/wire_bytes.h"
#include "plexus/publisher_gid.h"
#include "plexus/topic_qos.h"
#include "plexus/node_id.h"
#include "plexus/policy.h"
#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/fetch_latched.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// Pack a core subscriber_qos into the flat wire region (the wire layer carries no
// core dependency, so the lift lives here). The requested_flags bits are packed
// from the two request-side bools.
inline wire::subscribe_qos_region to_wire_region(const subscriber_qos &q)
{
    std::uint8_t flags = 0;
    if(q.requires_source_identity)
        flags |= wire::detail::k_qos_flag_requires_source_identity;
    if(q.requested_reliability_reliable)
        flags |= wire::detail::k_qos_flag_requested_reliable;
    if(q.rxo == rxo_mode::strict)
        flags |= wire::detail::k_qos_flag_rxo_strict;
    if(q.posture == attach_posture::strict)
        flags |= wire::detail::k_qos_flag_typed_strict;
    return wire::subscribe_qos_region{
            .durability            = static_cast<std::uint8_t>(q.durability_mode),
            .delivery_mode         = static_cast<std::uint8_t>(q.delivery_mode),
            .replay_depth          = q.replay_depth,
            .requested_flags       = flags,
            .requested_deadline_ns = q.requested_deadline_ns,
            .requested_lease_ns    = q.requested_lease_ns,
            .requested_priority    = q.requested_priority};
}

// Lift a decoded wire region back into a core subscriber_qos (the inverse of
// to_wire_region), unpacking the requested_flags bits.
inline subscriber_qos from_wire_region(const wire::subscribe_qos_region &r)
{
    return subscriber_qos{
            .durability_mode = static_cast<durability>(r.durability),
            .delivery_mode   = static_cast<delivery>(r.delivery_mode),
            .replay_depth    = r.replay_depth,
            .requires_source_identity
                = (r.requested_flags & wire::detail::k_qos_flag_requires_source_identity) != 0,
            .requested_reliability_reliable
                = (r.requested_flags & wire::detail::k_qos_flag_requested_reliable) != 0,
            .requested_deadline_ns = r.requested_deadline_ns,
            .requested_lease_ns    = r.requested_lease_ns,
            .requested_priority    = r.requested_priority,
            .rxo = (r.requested_flags & wire::detail::k_qos_flag_rxo_strict) != 0
                       ? rxo_mode::strict : rxo_mode::permissive,
            .posture = (r.requested_flags & wire::detail::k_qos_flag_typed_strict) != 0
                       ? attach_posture::strict : attach_posture::lenient};
}

// A peer's durable subscribe demand carries the subscriber's OWN requested qos so a
// deferred dial resurrects the real periods (not a default 0) — the local liveliness
// monitor reads them at the register seam. The fqn is the key; the qos is the
// subscriber's chosen choice stored once.
struct remembered_demand
{
    std::string    fqn;
    subscriber_qos qos;
    // The subscriber-declared type identity carried with the durable demand so a
    // reconnect resurrects the SAME type gate (std::nullopt = undeclared). Without it a
    // typed subscriber surviving a reconnect would silently re-subscribe untyped.
    std::optional<std::uint64_t> type_id;
};

// The hard cap on a latched topic's history depth (the KEEP_LAST-N ring capacity).
// A publisher-declared depth is an attacker-controlled count and N x max_payload is
// a resource-exhaustion vector, so the ring capacity is clamped to [1, this]; with
// max_payload itself bounded by the reassembler ceiling, the retained memory is
// bounded.
constexpr std::size_t k_history_depth_cap = 1024;

// The server-side hard cap on a fetch_latched (PULL) reply. max_samples arrives from
// an unverified peer, so the reply is capped at min(max_samples, ring.count(), this)
// — a huge or wrapping max_samples can never force more than count (<= N <= the
// history cap) sends, each walking only an owned, already-allocated ring frame.
constexpr std::size_t k_fetch_cap = 1024;

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
    using executor_type = typename Policy::executor_type;

    // A peer the forwarder fans toward: the channel plus its node-name key. The
    // public API takes peers by const& (no raw pointers).
    struct peer
    {
        channel_type &channel;
        std::string node_name;
    };

    // The borrowed executor is required (no default): the egress scheduler cannot post a
    // drain without one, so its absence is not meaningful. The logger keeps its default.
    // NOT noexcept — the scheduler's per-destination maps may allocate.
    explicit message_forwarder(executor_type executor, log::logger &logger = shared_null_logger())
        : m_logger(logger)
        , m_egress(executor)
    {
    }

    // attach: per-(peer, fqn) refcount gate. On the 0->1 transition it registers
    // the fan-out entry AND emits a wire::subscribe_request to the peer; returns
    // true. Subsequent attaches only bump the refcount and return false. The
    // subscriber's declared type_id (std::nullopt = undeclared) rides the subscribe
    // request so the producer can match it at subscribe time.
    bool attach(const peer &p, std::string_view fqn,
                const subscriber_qos &qos = subscriber_qos{},
                std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name, qos, type_id);
        record_remote_topic(p.node_name, fqn, qos, type_id);
        send_subscribe(p.channel, fqn, hash, type_id, qos);
        return true;
    }

    // attach_for_fanout: the producer-side reaction to an arriving subscribe. It
    // matches the subscriber's declared type_id (carried on the subscribe request)
    // against this topic's declared producer type_id. On a real mismatch (both sides
    // declared and unequal) it refuses: it replies subscribe_status::type_mismatch,
    // registers NO fan-out entry, and returns false. A match — or either side
    // undeclared (std::nullopt) — registers the fan-out entry and replies subscribed.
    // The type_name is carried on the wire for a future graph layer but matching
    // authority is the type_id alone; the type_hash/type_name string caps in
    // subscribe.h already bound the attacker-controlled fields, and a forged type_id
    // only yields a refusal (an equality compare, no parsing risk).
    bool attach_for_fanout(const peer &p, std::string_view fqn,
                           std::optional<std::uint64_t> subscriber_type_id = std::nullopt,
                           const subscriber_qos &sub_qos = subscriber_qos{})
    {
        auto hash = wire::fqn_topic_hash(fqn);
        if(type_id_mismatch(hash, subscriber_type_id))
        {
            auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = wire::subscribe_status::type_mismatch});
            send_control(p.channel, wire::msg_type::subscribe_response, resp);
            return false;
        }
        // The strict typed attach posture: a subscriber that declared a type AND chose
        // strict refuses to bind to a producer that declared NO type — the verdict it
        // would otherwise silently miss. Ordered AFTER type_mismatch (a declared-vs-
        // declared mismatch is the more specific verdict). Refusal mirrors the
        // type_mismatch shape: reply type_undeclared, register no fan-out entry, return
        // false, session stays up.
        if(sub_qos.posture == attach_posture::strict && subscriber_type_id
           && !m_registry.producer_type_id(hash))
        {
            auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = wire::subscribe_status::type_undeclared});
            send_control(p.channel, wire::msg_type::subscribe_response, resp);
            return false;
        }
        // The request-vs-offered compatibility gate, generalizing the one-field
        // type_mismatch refusal to the full QoS matrix. Both halves co-locate here:
        // the subscriber's REQUESTED sub_qos arrived off the wire, the topic's OFFERED
        // qos (incl. the off-wire offered periods) + the source-identity offer are read
        // locally. A refusing verdict mirrors the type_mismatch shape exactly — reply
        // the matching status, register NO fan-out entry, return false, leave the
        // session up (a per-topic refusal, never a peer teardown).
        const topic_qos offered = m_registry.qos_for(hash);
        const bool offers_sid   = m_registry.offers_source_identity(hash);
        const auto rxo = io::rxo_check(offered, offers_sid, sub_qos);
        if(rxo.verdict == io::rxo_verdict::incompatible_qos
           || rxo.verdict == io::rxo_verdict::source_identity_incompatible)
        {
            auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = status_of(rxo.verdict)});
            send_control(p.channel, wire::msg_type::subscribe_response, resp);
            return false;
        }
        if(m_registry.bump_refcount(p.node_name, fqn) != 1u)
            return false;
        m_registry.add_subscriber(hash, fqn, p.channel, p.node_name, sub_qos, subscriber_type_id);
        // A degraded verdict ADMITS (the consumer chose permissive) but the reply
        // carries the surfaced degraded-field set so the accept is non-silent; a clean
        // compatible verdict replies the byte-identical bare `subscribed`.
        auto resp = rxo.verdict == io::rxo_verdict::degraded
            ? wire::encode_subscribe_response({.topic_hash = hash,
                                               .status = wire::subscribe_status::subscribed_degraded,
                                               .has_degraded = true,
                                               .degraded_flags = rxo.degraded_fields})
            : wire::encode_subscribe_response({.topic_hash = hash,
                                               .status = wire::subscribe_status::subscribed});
        send_control(p.channel, wire::msg_type::subscribe_response, resp);
        replay_if_latched(p, hash);
        return true;
    }

    // remember_demand: record a peer's durable subscribe demand WITHOUT a wire emit.
    // The engine calls this the moment subscribe is requested — possibly before the
    // session exists (an async dial has not completed) — so the demand is durable and
    // the session resurrects it through the counted path when it completes. No wire,
    // no fan-out registration here: attach (driven by the session) does both later.
    void remember_demand(const std::string &node_name, std::string_view fqn,
                         const subscriber_qos &qos = subscriber_qos{},
                         std::optional<std::uint64_t> type_id = std::nullopt)
    {
        record_remote_topic(node_name, fqn, qos, type_id);
    }

    // forget_remembered_demand: drop a peer's durable subscribe demand WITHOUT a wire
    // emit — the inverse of remember_demand for a demand that never attached (no session
    // completed, so there is no refcount to detach and no wire unsubscribe to send). The
    // attached case forgets its own record on detach's 1->0 transition; this seam covers
    // the engine's retire of a standing demand whose dial never completed.
    void forget_remembered_demand(const std::string &node_name, std::string_view fqn)
    {
        forget_remote_topic(node_name, fqn);
    }

    // declare: mark a topic with a publisher-declared qos once. A latched topic
    // retains its last published frame and replays it to late subscribers.
    // An optional producer type_id (std::nullopt = undeclared) is the subscribe-time
    // match authority: a subscriber declaring a different type_id is refused with
    // type_mismatch. emit_source_identity (producer-offered) opts this topic
    // into per-frame source-identity carriage — its publishes set the gid flag and
    // carry a varint endpoint counter the receiver pairs with the session peer's
    // node_id; a subscriber-side "require source identity" refusal is a separate,
    // later compatibility concern. The hot publish(fqn, bytes) signature is unchanged.
    void declare(std::string_view fqn, topic_qos qos,
                 std::optional<std::uint64_t> producer_type_id = std::nullopt,
                 bool emit_source_identity = false)
    {
        m_registry.declare(wire::fqn_topic_hash(fqn), fqn, qos, producer_type_id, emit_source_identity);
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
                 std::uint64_t session_id = 0)
    {
        auto hash = wire::fqn_topic_hash(fqn);
        const auto *topic = m_registry.entry_for(hash);
        const auto *subs =
            topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        const bool latched = qos.latch;
        const locality reach = qos.reach;
        // A null topic implies subs == nullptr and latched == false, so past this
        // return `topic` is always valid.
        if(subs == nullptr && !latched)
            return;   // neither a subscriber nor a latch reason to frame

        wire::unidirectional_header uhdr{
                .source     = wire::endpoint_source_type::publisher,
                .sequence   = m_next_sequence++,
                .topic_hash = hash
        };
        // Source identity (Option 2: producer-offered). When this topic declared
        // emit_source_identity, frame the gid flag + a varint endpoint counter; the
        // receiver reconstructs publisher_gid as session.node_id ‖ counter. Absent →
        // 0 B and a byte-identical v3-no-flag frame. Per-topic, decided at framing
        // time, so the frame-ONCE-fan-to-N invariant holds (one buffer for all subs).
        const std::optional<std::uint64_t> counter =
            topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
        // Frame ONCE into the reused scratch buffers: after the first publish
        // grows them, resize() reuses capacity so steady-state publishes do not
        // allocate (the SLICE-3 no-hot-path-allocation property, designed in here).
        wire::encode_unidirectional_into(m_inner_scratch, uhdr, payload, counter);

        wire::frame_header fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                .session_id   = session_id,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = m_inner_scratch.size()
        };
        wire::encode_frame_into(m_frame_scratch, fhdr, m_inner_scratch);

        // The SINGLE fan-out confinement choke point: send to a subscriber only when
        // the topic's reach mask shares a bit with the subscriber's cached tier. Default
        // reach=any sends to all (no behavior change); a confined mask drops an off-scope
        // tier and never leaks (fail-closed access control). The reach gate is UNCHANGED
        // and stays BEFORE the egress enqueue. The scheduler governs the LIVE fan-out
        // ONLY — control frames and latch replay use direct channel.send (see below). An
        // inproc/sink channel (no backpressured()) short-circuits through the scheduler to
        // a direct synchronous send, byte-identical to a bandless fan-out.
        //
        // The scheduler ENFORCES the publisher's declared overflow policy at the
        // destination band: qos.priority selects the band/drain order, qos.congestion
        // decides a saturated band's outcome (block refuses, drop_oldest evicts the
        // oldest resident frame, drop_newest refuses the new one). The reach gate,
        // frame-once, demand-driven no-op, and the latch/control/fetch direct-send bypass
        // are all unchanged.
        const std::size_t band = detail::band_of(qos.priority);
        if(subs != nullptr)
            for(const auto &sub : *subs)
                if(any_set(reach, sub.tier))
                    m_egress.enqueue(*sub.channel, band, qos.congestion, m_frame_scratch);

        retain_if_latched(hash, qos);
    }

    // publish_object: the zero-serialization SIBLING of publish (never the hot
    // publish signature). It fans a refcounted object handle to each subscriber
    // behind the SAME confinement gate, then falls back to the byte path for any
    // subscriber the fast path cannot serve. encode is invoked AT MOST ONCE per
    // publish, lazily, the first time any byte need appears (a bytes-family
    // subscriber, a tag mismatch, a non-process tier, an object-laneless channel, or
    // a latched topic). The carrier's sequence is stamped from the SAME per-topic
    // counter publish uses so mixed-path sequences stay coherent.
    //
    // Reference protocol: the CALLER owns one reference on entry; each fast-path
    // send_object addrefs through the bus, and this verb releases the caller's
    // reference once after the fan loop and latch retention — so the slot is balanced
    // whether it fast-pathed to many subscribers, fell back to bytes, or reached none.
    template <typename EncodeFn>
    void publish_object(std::string_view fqn, object_carrier carrier, EncodeFn &&encode,
                        std::uint64_t session_id = 0)
    {
        auto hash = wire::fqn_topic_hash(fqn);
        const auto *topic = m_registry.entry_for(hash);
        const auto *subs =
            topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        const bool latched = qos.latch;
        const locality reach = qos.reach;

        carrier.topic_hash = hash;
        carrier.sequence = m_next_sequence++;
        if(carrier.source_timestamp == 0)
            carrier.source_timestamp = wire::now_timestamp_ns();

        bool encoded = false;
        const auto ensure_encoded_once = [&] {
            if(encoded)
                return;
            encoded = true;
            std::span<const std::byte> bytes = std::forward<EncodeFn>(encode)();
            wire::unidirectional_header uhdr{
                    .source     = wire::endpoint_source_type::publisher,
                    .sequence   = carrier.sequence,
                    .topic_hash = hash
            };
            const std::optional<std::uint64_t> counter =
                topic != nullptr && topic->emit_source_identity ? topic->endpoint_counter
                                                                : std::nullopt;
            wire::encode_unidirectional_into(m_inner_scratch, uhdr, bytes, counter);
            wire::frame_header fhdr{
                    .type         = wire::msg_type::unidirectional,
                    .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                    .session_id   = session_id,
                    .timestamp_ns = carrier.source_timestamp,
                    .payload_len  = m_inner_scratch.size()
            };
            wire::encode_frame_into(m_frame_scratch, fhdr, m_inner_scratch);
        };

        const std::size_t band = detail::band_of(qos.priority);
        if(subs != nullptr)
            for(const auto &sub : *subs)
            {
                if(!any_set(reach, sub.tier))
                    continue;
                // The object-lane send is gated at COMPILE TIME on the channel exposing
                // it: a channel without send_object (every remote/local stream channel —
                // the lane is process-tier only) never instantiates the call, so a typed
                // publisher compiles over it and always takes the byte path here.
                if constexpr(requires(channel_type &c) { c.send_object(carrier); })
                {
                    if(eligible_for_object(sub, carrier))
                    {
                        sub.channel->send_object(carrier);
                        continue;
                    }
                }
                ensure_encoded_once();
                m_egress.enqueue(*sub.channel, band, qos.congestion, m_frame_scratch);
            }

        // A latched topic's history ring is the byte path's memory: force exactly one
        // encode even when every live subscriber fast-pathed, so a late bytes joiner
        // replays a real frame (retain_if_latched retains the just-framed bytes).
        if(latched)
        {
            ensure_encoded_once();
            retain_if_latched(hash, qos);
        }

        release(carrier);
    }

    // detach: per-(peer, fqn) refcount gate. On the 1->0 transition it removes
    // the fan-out entry AND emits a wire::unsubscribe_request; returns true.
    bool detach(const peer &p, std::string_view fqn)
    {
        if(m_registry.drop_refcount(p.node_name, fqn) != 0u)
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_registry.remove_subscriber(hash, p.channel);
        m_egress.remove(p.channel);
        forget_remote_topic(p.node_name, fqn);
        auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
        send_control(p.channel, wire::msg_type::unsubscribe, req);
        return true;
    }

    // detach_all: drop all the peer's fan-out entries and refcounts; NO wire emit
    // (the peer is gone, the wire has already torn down). The remembered subscribe
    // demand (m_remote_topics) is DELIBERATELY retained: a transport teardown is not
    // an unsubscribe, so the demand survives to drive reconnect resurrection
    // (drain_for / the session's resubscribe_all). A genuine unsubscribe forgets its
    // topic on detach()'s 1->0 transition, so the record still prunes on real demand
    // loss.
    void detach_all(const peer &p)
    {
        m_registry.remove_peer(p.node_name, p.channel);
        m_egress.remove(p.channel);
    }

    // remembered_topics: the durable subscribe demand for a peer's node_name — the
    // remote topics it has attached that have not been explicitly unsubscribed. A pure
    // read (no wire emit, no mutation) so the forwarder stays readiness-agnostic: the
    // session reads this to resurrect its counted subscribes on reconnect. Returns an
    // empty vector for an unknown node_name.
    const std::vector<remembered_demand> &remembered_topics(const std::string &node_name) const
    {
        static const std::vector<remembered_demand> empty;
        auto it = m_remote_topics.find(node_name);
        return it == m_remote_topics.end() ? empty : it->second;
    }

    // drain_for: re-emit a wire::subscribe for each remote topic rooted at the
    // peer's node name (subscription resurrection on reconnect).
    void drain_for(const peer &p)
    {
        auto it = m_remote_topics.find(p.node_name);
        if(it == m_remote_topics.end())
            return;
        for(const auto &demand : it->second)
            send_subscribe(p.channel, demand.fqn, wire::fqn_topic_hash(demand.fqn),
                           demand.type_id, demand.qos);
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
    void deliver(const peer &p, std::span<const std::byte> inner,
                 const node_id &source_node_id, bool has_source_identity,
                 OnMessage &&on_message)
    {
        (void)p;   // identity symmetry with the procedure receive tail; resolution is by topic_hash
        // has_source_identity mirrors the frame's gid flag. The bytes-only tail discards
        // the counter, but it MUST still pass the flag: the producer emits the varint per
        // ITS topic declaration, independent of which receive callback is set, so the data
        // span is only correct when the flag is honored on decode.
        auto decoded = wire::decode_unidirectional(inner, has_source_identity);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_registry.fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data);
    }

    // The metadata-bearing receive tail: identical resolution to the 2-arg deliver, but
    // it finishes the message_info the session began at on_receive (where the now-stripped
    // frame_header was live) and hands it to a 3-arg callback. The forwarder fills
    // publication_sequence (carried INSIDE the inner payload it alone decodes) and, when
    // the gid flag rode the frame, source_identity; the header-derived fields
    // (source_timestamp, reception_timestamp, from_intra_process) were already stamped by
    // the session.
    //
    // DIRECT-DELIVERY INVARIANT (the gid rests on it): the gid's node_id half is the
    // PINNED session peer's node_id (source_node_id) — the peer at the other end of THIS
    // channel — NOT a node_id taken from the frame. The forwarder fans to subscribed
    // channels and never relays across peer boundaries, so source == the session peer;
    // the wire therefore carries only the endpoint counter, and a peer can claim counters
    // ONLY within its own node_id namespace (structural anti-spoof — a forged counter
    // cannot impersonate another node). A future relay/bridge/store-and-forward topology
    // would break this invariant and MUST carry full origin identity locally on those frames.
    template <typename OnMessage>
    void deliver(const peer &p, std::span<const std::byte> inner, message_info info,
                 const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
        (void)p;   // identity symmetry with the procedure receive tail; resolution is by topic_hash
        auto decoded = wire::decode_unidirectional(inner, has_source_identity);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_registry.fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        info.publication_sequence = decoded->header.sequence;
        if(decoded->endpoint_counter)
            info.source_identity = publisher_gid{source_node_id, *decoded->endpoint_counter};
        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data, info);
    }

    // The receive-path liveness stamp seam: invoked from BOTH deliver overloads after
    // a successful decode, where the topic_hash is already in hand (no second decode of
    // untrusted bytes). The endpoint is the PINNED session peer's node_id (the direct-
    // delivery invariant), never frame-supplied. The hook is a plain settable callback
    // the engine wires to its liveliness monitor's stamp_data; absent = no stamp, so the
    // forwarder stays monitor-agnostic. A plain call, never a timer arm.
    void set_on_data_stamp(plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> hook)
    {
        m_on_data_stamp = std::move(hook);
    }

    // fetch_latched: the consumer-paced PULL reply. Replay up to
    // min(max_samples, ring.count(), k_fetch_cap) retained frames — the most-recent
    // that many, oldest->newest — to the REQUESTING peer's channel ONLY (never the
    // fan-out loop). max_samples is attacker-controlled, so the cap bounds the reply
    // to owned, already-allocated ring frames; an unknown/unlatched/empty topic
    // sends zero frames (no crash).
    void fetch_latched(const peer &p, std::uint64_t topic_hash, std::uint32_t max_samples)
    {
        auto it = m_retained.find(topic_hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        const std::size_t limit =
            std::min<std::size_t>({static_cast<std::size_t>(max_samples), it->second.count(), k_fetch_cap});
        replay_window(p, it->second, limit);
    }

    // Resolve a topic_hash back to its fqn — the object-lane receive tail's analog of
    // the byte tail's internal fqn_for (the bytes path resolves inside deliver). The
    // subscriber side recorded the hash->fqn mapping at demand time. Empty when the
    // hash names no attached topic.
    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        return m_registry.fqn_for(topic_hash);
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

    // retain_if_latched: on a latched publish, push the just-framed complete frame
    // into the per-topic KEEP_LAST-N history ring. The ring is sized ONCE from the
    // declared depth (clamped to [1, k_history_depth_cap] so N x max_payload cannot
    // be driven to OOM by a careless/hostile declare), then push() reuses each slot's
    // grown capacity — alloc-free after warm-up. A capacity-1 ring is byte-identical
    // to the pre-ring single slot: last-writer-wins per topic_hash. The ring slots
    // OWN their bytes; they never alias m_frame_scratch (the next publish overwrites).
    void retain_if_latched(std::uint64_t hash, const topic_qos &qos)
    {
        if(!qos.latch)
            return;
        auto &ring = m_retained[hash];
        ring.resize_to(std::clamp<std::size_t>(qos.depth, 1, k_history_depth_cap));
        ring.push(m_frame_scratch);
    }

    // replay_if_latched: when a new subscriber attaches to a latched topic with a
    // retained history, PUSH frames to ONLY the new peer (not the fan-out loop) as
    // ordinary data frames, after the subscribe_response. A latched-but-never-
    // published topic retains nothing, so it replays nothing.
    //
    // The new subscriber's stored durability choice gates the replay: `none` declines
    // every retained frame; `latest` takes the single newest frame; `all` takes the
    // retained history oldest->newest, capped to the most-recent replay_depth frames
    // when that is non-zero (min(count, replay_depth)).
    void replay_if_latched(const peer &p, std::uint64_t hash)
    {
        if(!m_registry.qos_for(hash).latch)
            return;
        auto it = m_retained.find(hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        const subscriber_qos sub = m_registry.qos_for_subscriber(hash, p.channel);
        switch(sub.durability_mode)
        {
        case durability::none:
            return;
        case durability::latest:
            p.channel.send(it->second.newest());
            return;
        case durability::all:
        {
            const std::size_t count = it->second.count();
            const std::size_t limit = sub.replay_depth
                                          ? std::min<std::size_t>(count, sub.replay_depth)
                                          : count;
            replay_window(p, it->second, limit);
            return;
        }
        }
    }

    // Send the most-recent `limit` frames of a ring to one peer, oldest->newest. The
    // shared replay window for durability=all and the fetch_latched PULL reply: both
    // walk only owned, already-allocated ring frames to the requesting peer's channel.
    void replay_window(const peer &p, const detail::history_ring &ring, std::size_t limit)
    {
        const std::size_t count = ring.count();
        for(std::size_t i = count - limit; i < count; ++i)
            p.channel.send(ring.oldest_to_newest(i));
    }

    // eligible_for_object: a subscriber takes the zero-serialization fast path only on
    // a same-process tier, over a channel that actually exposes the object lane
    // (if-constexpr capability on the concrete channel type — never a byte_channel
    // concept verb, mirroring the egress can_poll precedent), AND with a stored
    // type_id matching the carrier's wire tag. A remote/local channel structurally
    // lacks send_object, so the constexpr branch makes it ineligible at compile time.
    static bool eligible_for_object(const typename subscriber_registry<channel_type>::subscriber &sub,
                                    const object_carrier &carrier)
    {
        if constexpr(requires(channel_type &c) { c.send_object(carrier); })
            return sub.tier == locality::process && sub.type_id && *sub.type_id == carrier.type_tag;
        else
            return false;
    }

    // type_id_mismatch: a refusal is warranted only when BOTH the producer and the
    // subscriber declared a type_id and they differ. Either side undeclared
    // (std::nullopt) is never a mismatch — absence is a distinct state, not a zero
    // type_id that would false-refuse.
    bool type_id_mismatch(std::uint64_t hash, std::optional<std::uint64_t> subscriber_type_id) const
    {
        const auto producer_type_id = m_registry.producer_type_id(hash);
        if(!producer_type_id || !subscriber_type_id)
            return false;
        return *producer_type_id != *subscriber_type_id;
    }

    // Map a refusing RxO verdict to its wire subscribe_status. Only the two refusing
    // verdicts reach here; compatible/degraded are admitted (a separate reply path).
    static wire::subscribe_status status_of(io::rxo_verdict v)
    {
        return v == io::rxo_verdict::source_identity_incompatible
                   ? wire::subscribe_status::source_identity_incompatible
                   : wire::subscribe_status::incompatible_qos;
    }

    void send_subscribe(channel_type &channel, std::string_view fqn, std::uint64_t hash,
                        std::optional<std::uint64_t> type_id = std::nullopt,
                        const subscriber_qos &sub_qos = subscriber_qos{})
    {
        // The wire carries the type_id in the already-present type_hash field; 0 is
        // the undeclared sentinel (an undeclared subscriber writes 0, which the
        // producer reads back as "no type declared" — never a mismatch).
        wire::subscribe_request req{
                .fqn        = std::string{fqn},
                .type_name  = {},
                .topic_hash = hash,
                .type_hash  = type_id.value_or(0),
                .source     = wire::endpoint_source_type::publisher
        };
        // Carry the choice OUT only when it differs from the friendly default, so a
        // default subscribe stays byte-identical to the pre-region encoding.
        if(!(sub_qos == subscriber_qos{}))
        {
            req.has_qos = true;
            req.qos = to_wire_region(sub_qos);
        }
        auto bytes = wire::encode_subscribe_request(req);
        send_control(channel, wire::msg_type::subscribe, bytes);
    }

    void record_remote_topic(const std::string &node_name, std::string_view fqn,
                             const subscriber_qos &qos,
                             std::optional<std::uint64_t> type_id = std::nullopt)
    {
        auto &topics = m_remote_topics[node_name];
        for(const auto &existing : topics)
            if(existing.fqn == fqn)
                return;   // idempotent: a re-record keeps the first stored qos + type_id
        topics.emplace_back(remembered_demand{std::string{fqn}, qos, type_id});
    }

    // Forget a remembered topic on a genuine unsubscribe (detach's 1->0 transition) so
    // the durable demand record reflects only live demand — a topic the user dropped
    // must not be resurrected on reconnect. Erasing the last topic empties the node's
    // entry; the next attach re-creates it.
    void forget_remote_topic(const std::string &node_name, std::string_view fqn)
    {
        auto it = m_remote_topics.find(node_name);
        if(it == m_remote_topics.end())
            return;
        std::erase_if(it->second, [&](const remembered_demand &d) { return d.fqn == fqn; });
        if(it->second.empty())
            m_remote_topics.erase(it);
    }

    // Stamp the endpoint's last-data-seen for this topic (deadline + presence), when
    // the engine has wired the monitor. A plain store on the receive path — no timer.
    void stamp_received(const node_id &source_node_id, std::uint64_t topic_hash)
    {
        if(m_on_data_stamp)
            m_on_data_stamp(source_node_id, topic_hash);
    }

    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger &m_logger;
    subscriber_registry<channel_type> m_registry;
    detail::egress_scheduler<channel_type, Policy> m_egress;
    std::unordered_map<std::string, std::vector<remembered_demand>> m_remote_topics;
    std::vector<std::byte> m_inner_scratch;
    std::vector<std::byte> m_frame_scratch;
    std::vector<std::byte> m_control_scratch;
    std::unordered_map<std::uint64_t, detail::history_ring> m_retained;
    std::uint64_t m_next_sequence{0};
    plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> m_on_data_stamp;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/qos_rxo.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/observation_events.h"
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

// Durable subscribe demand: stores the subscriber's OWN requested qos so a deferred
// dial resurrects the real periods (not a default 0). The fqn is the key.
struct remembered_demand
{
    std::string    fqn;
    subscriber_qos qos;
    // std::nullopt = undeclared. Without it a typed subscriber surviving a reconnect
    // would silently re-subscribe untyped.
    std::optional<std::uint64_t> type_id;
};

// A publisher-declared depth is an attacker-controlled count and N x max_payload is a
// resource-exhaustion vector, so the ring capacity is clamped to [1, this].
constexpr std::size_t k_history_depth_cap = 1024;

// max_samples arrives from an unverified peer, so the reply is capped at
// min(max_samples, ring.count(), this) — never more than count owned ring frames.
constexpr std::size_t k_fetch_cap = 1024;

// >200 LOC: the attach/publish/deliver/replay verbs are one cohesive pub/sub engine over
// a shared registry + scratch state; splitting them would scatter that state behind seams.
//
// Payload-opaque pub/sub engine. Refcount-gated (one subscribe per uninterrupted attach
// run), frames each publish ONCE and shares the single owning buffer across subscribers,
// and warn-and-drops a malformed frame on the receive tail through the injected cold-path
// logger& (default shared null_logger). It never interprets the payload.
template <typename Policy>
    requires plexus::Policy<Policy>
class message_forwarder
{
public:
    using channel_type = typename Policy::byte_channel_type;
    using endpoint_type = subscription_endpoint<channel_type>;
    using peer = typename endpoint_type::peer;

    // global_default is the node-level per-message size default the RxO size relation
    // resolves an offered topic's 0=unset max against — the SAME node-level value the
    // data-path transports resolve against, so a remote subscribe is admitted against
    // one consistent ceiling rather than a forwarder-local constant that could drift.
    // It is required-with-default: the shipped constant is the meaningful fallback.
    explicit message_forwarder(std::size_t global_default = io::global_default_max_message_bytes,
                               log::logger &logger = shared_null_logger())
        : m_logger(logger), m_global_default(global_default)
    {
    }

    // Per-(peer, fqn) refcount gate: only the 0->1 transition registers the fan-out
    // entry and emits the wire subscribe_request; later attaches just bump and return
    // false. The subscriber's declared type_id rides the request for subscribe-time match.
    bool attach(const peer &p, std::string_view fqn,
                const subscriber_qos &qos = subscriber_qos{},
                std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, qos, type_id);
        record_remote_topic(p.node_name, fqn, qos, type_id);
        send_subscribe(p.channel, fqn, hash, type_id, qos);
        emit_qos_change(qos_edge::accepted, hash, qos, rxo_verdict::compatible, type_id);
        return true;
    }

    // The producer-side reaction to an arriving subscribe: matches the subscriber's
    // declared type_id against this topic's producer type_id, then runs the full QoS
    // compatibility gate. Each refusal replies a status, registers NO fan-out entry, and
    // returns false (a per-topic refusal, never a peer teardown). Matching authority is
    // the type_id alone; a forged type_id only yields a refusal (an equality compare, no
    // parsing risk), and subscribe.h already caps the attacker-controlled string fields.
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
        // Strict typed posture: a typed-and-strict subscriber refuses an untyped producer.
        // Ordered AFTER type_mismatch (a declared-vs-declared mismatch is more specific).
        if(sub_qos.posture == attach_posture::strict && subscriber_type_id
           && !m_endpoint.registry().producer_type_id(hash))
        {
            auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = wire::subscribe_status::type_undeclared});
            send_control(p.channel, wire::msg_type::subscribe_response, resp);
            return false;
        }
        // The request-vs-offered compatibility gate over the full QoS matrix: the
        // subscriber's REQUESTED sub_qos arrived off the wire; the topic's OFFERED qos and
        // source-identity offer are read locally.
        const topic_qos offered = m_endpoint.registry().qos_for(hash);
        const bool offers_sid   = m_endpoint.registry().offers_source_identity(hash);
        const auto rxo = io::rxo_check(offered, offers_sid, m_global_default, sub_qos);
        if(rxo.verdict == io::rxo_verdict::incompatible_qos
           || rxo.verdict == io::rxo_verdict::source_identity_incompatible)
        {
            auto resp = wire::encode_subscribe_response(
                {.topic_hash = hash, .status = status_of(rxo.verdict)});
            send_control(p.channel, wire::msg_type::subscribe_response, resp);
            emit_qos_change(qos_edge::refused, hash, sub_qos, rxo.verdict, subscriber_type_id);
            return false;
        }
        if(m_endpoint.registry().bump_refcount(p.node_name, fqn) != 1u)
            return false;
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, sub_qos, subscriber_type_id);
        emit_qos_change(rxo.verdict == io::rxo_verdict::degraded ? qos_edge::degraded : qos_edge::accepted,
                        hash, sub_qos, rxo.verdict, subscriber_type_id);
        // A degraded verdict ADMITS (the consumer chose permissive) but the reply carries
        // the degraded-field set so the accept is non-silent.
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

    // Record durable subscribe demand WITHOUT a wire emit — the engine calls this when
    // subscribe is requested, possibly before the session exists (async dial pending), so
    // the session resurrects it through the counted path on completion.
    void remember_demand(const std::string &node_name, std::string_view fqn,
                         const subscriber_qos &qos = subscriber_qos{},
                         std::optional<std::uint64_t> type_id = std::nullopt)
    {
        record_remote_topic(node_name, fqn, qos, type_id);
    }

    // The inverse of remember_demand for a demand that never attached (no refcount to
    // detach, no wire unsubscribe to send). The attached case forgets its own record on
    // detach's 1->0 transition.
    void forget_remembered_demand(const std::string &node_name, std::string_view fqn)
    {
        forget_remote_topic(node_name, fqn);
    }

    // Mark a topic with a publisher-declared qos once. producer_type_id is the
    // subscribe-time match authority. emit_source_identity opts the topic into per-frame
    // source-identity carriage: publishes set the gid flag and carry a varint endpoint
    // counter the receiver pairs with the session peer's node_id.
    void declare(std::string_view fqn, topic_qos qos,
                 std::optional<std::uint64_t> producer_type_id = std::nullopt,
                 bool emit_source_identity = false)
    {
        m_endpoint.registry().declare(wire::fqn_topic_hash(fqn), fqn, qos, producer_type_id, emit_source_identity);
    }

    void latch(std::string_view fqn)
    {
        declare(fqn, topic_qos{.latch = true, .depth = 1});
    }

    // Frame ONCE and fan the single owning buffer to each subscribed channel; no
    // subscriber means no send (demand-driven). session_id is passed per send (NOT a
    // member) because a node-shared forwarder fans to many peers, each with its own epoch.
    void publish(std::string_view fqn, std::span<const std::byte> payload,
                 std::uint64_t session_id = 0)
    {
        auto hash = wire::fqn_topic_hash(fqn);
        const auto *topic = m_endpoint.registry().entry_for(hash);
        const auto *subs =
            topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        const bool latched = qos.latch;
        const locality reach = qos.reach;
        // A null topic implies subs == nullptr and latched == false, so past this return
        // `topic` is always valid.
        if(subs == nullptr && !latched)
            return;

        wire::unidirectional_header uhdr{
                .source     = wire::endpoint_source_type::publisher,
                .sequence   = m_endpoint.next_sequence(),
                .topic_hash = hash
        };
        // When the topic offers source identity, frame the gid flag + a varint endpoint
        // counter the receiver pairs with the session node_id; absent yields a
        // byte-identical no-flag frame. Decided once at framing time, so the
        // frame-ONCE-fan-to-N invariant holds (one buffer for all subs).
        const std::optional<std::uint64_t> counter =
            topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
        wire::frame_header fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                .session_id   = session_id,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = 0   // set inside the one-pass encode from the framed region size
        };
        // Frame ONCE into a single owning buffer; the owner is addref-shared to each
        // subscriber's band slot (frame-once-fan-to-N) and rides the band → channel send
        // queue with no further copy.
        const wire_bytes<> framed = frame_owned(fhdr, uhdr, payload, counter);

        // The published tap fires ONCE after framing, borrowing the framed owner (an
        // addref of the single per-publish buffer, never a copy); the delivered tap fires
        // ONCE per fanned destination below. Both stay posted through the engine sink.
        emit_published(fqn, framed);

        const message_info pub_info{.publication_sequence = uhdr.sequence,
                                    .source_timestamp = fhdr.timestamp_ns};

        // The fan-out confinement gate: send to a subscriber only when the topic's reach
        // mask shares a bit with the subscriber's cached tier (fail-closed access control).
        // The reach gate stays BEFORE the egress enqueue. The scheduler governs the LIVE
        // fan-out only; control frames and latch replay take direct channel.send.
        const std::size_t band = detail::band_of(qos.priority);
        if(subs != nullptr)
            for(const auto &sub : *subs)
                if(any_set(reach, sub.tier))
                {
                    const detail::drop_cause cause =
                        m_egress.enqueue(*sub.channel, band, qos.congestion, framed);
                    if(cause != detail::drop_cause::none)
                        shed(hash, band, cause, sub.tier);
                    emit_delivered(fqn, pub_info, framed);
                }

        retain_if_latched(hash, qos, framed);
    }

    // The zero-serialization sibling of publish: fans a refcounted object handle behind
    // the SAME confinement gate, falling back to the byte path for any subscriber the fast
    // path cannot serve. encode is invoked AT MOST ONCE, lazily, the first time any byte
    // need appears. The carrier's sequence is stamped from the same counter publish uses.
    //
    // Reference protocol: the CALLER owns one reference on entry; each fast-path
    // send_object addrefs through the bus, and this verb releases the caller's reference
    // once after the fan loop — so the slot is balanced on every path.
    template <typename EncodeFn>
    void publish_object(std::string_view fqn, object_carrier carrier, EncodeFn &&encode,
                        std::uint64_t session_id = 0)
    {
        auto hash = wire::fqn_topic_hash(fqn);
        const auto *topic = m_endpoint.registry().entry_for(hash);
        const auto *subs =
            topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        const bool latched = qos.latch;
        const locality reach = qos.reach;

        carrier.topic_hash = hash;
        carrier.sequence = m_endpoint.next_sequence();
        // Skip the source-stamp clock read when no attached subscriber wants message_info,
        // leaving source_timestamp == 0 (the "not stamped" sentinel the ==0 keying relies
        // on). The latch is local-only, so a remote-decoded subscriber always reads true.
        if(carrier.source_timestamp == 0 && (topic == nullptr || topic->any_subscriber_wants_info))
            carrier.source_timestamp = wire::now_timestamp_ns();

        // Framed lazily AT MOST ONCE into a single owning buffer the first time any byte
        // need appears, then addref-shared across every byte-path subscriber + the latch
        // ring (frame-once-fan-to-N).
        wire_bytes<> framed;
        bool encoded = false;
        const auto encode_once = [&] {
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
            wire::frame_header fhdr{
                    .type         = wire::msg_type::unidirectional,
                    .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                    .session_id   = session_id,
                    .timestamp_ns = carrier.source_timestamp,
                    .payload_len  = 0   // set inside the one-pass encode from the framed region size
            };
            framed = frame_owned(fhdr, uhdr, bytes, counter);
        };

        // The typed loan path carries NO payload, so its observation view is ENVELOPE-ONLY
        // by default (the carrier's metadata, no payload encode): recording payload on the
        // zero-serialization lane is a sovereign opt-in, never silently imposed. The
        // published tap fires once, the delivered tap once per fanned destination below.
        const message_info obj_info{.publication_sequence = carrier.sequence,
                                    .source_timestamp = carrier.source_timestamp};
        emit_published(fqn, message_view{});

        const std::size_t band = detail::band_of(qos.priority);
        if(subs != nullptr)
            for(const auto &sub : *subs)
            {
                if(!any_set(reach, sub.tier))
                    continue;
                // Gated at COMPILE TIME: a channel without send_object (every remote/local
                // stream channel — the lane is process-tier only) never instantiates the
                // call and always takes the byte path here.
                if constexpr(requires(channel_type &c) { c.send_object(carrier); })
                {
                    if(eligible_for_object(sub, carrier))
                    {
                        sub.channel->send_object(carrier);
                        emit_delivered(fqn, obj_info, message_view{});
                        continue;
                    }
                }
                encode_once();
                const detail::drop_cause cause =
                    m_egress.enqueue(*sub.channel, band, qos.congestion, framed);
                if(cause != detail::drop_cause::none)
                    shed(hash, band, cause, sub.tier);
                emit_delivered(fqn, obj_info, message_view{});
            }

        // Force one encode even when every live subscriber fast-pathed, so a late bytes
        // joiner replays a real frame from the history ring.
        if(latched)
        {
            encode_once();
            retain_if_latched(hash, qos, framed);
        }

        release(carrier);
    }

    // Per-(peer, fqn) refcount gate: only the 1->0 transition removes the fan-out entry
    // and emits a wire unsubscribe_request.
    bool detach(const peer &p, std::string_view fqn)
    {
        if(!m_endpoint.detach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().remove_subscriber(hash, p.channel);
        m_egress.remove(p.channel);
        forget_remote_topic(p.node_name, fqn);
        auto req = wire::encode_unsubscribe_request({.topic_hash = hash});
        send_control(p.channel, wire::msg_type::unsubscribe, req);
        return true;
    }

    // Drop all the peer's fan-out entries and refcounts; NO wire emit (the peer is gone).
    // The remembered demand is DELIBERATELY retained: a transport teardown is not an
    // unsubscribe, so it survives to drive reconnect resurrection. A genuine unsubscribe
    // prunes its own record on detach's 1->0 transition.
    void detach_all(const peer &p)
    {
        m_endpoint.remove_peer(p);
        m_egress.remove(p.channel);
    }

    // The durable subscribe demand for a peer's node_name (a pure read, no wire emit) so
    // the forwarder stays readiness-agnostic: the session reads this to resurrect its
    // counted subscribes on reconnect. Empty vector for an unknown node_name.
    const std::vector<remembered_demand> &remembered_topics(const std::string &node_name) const
    {
        static const std::vector<remembered_demand> empty;
        auto it = m_remote_topics.find(node_name);
        return it == m_remote_topics.end() ? empty : it->second;
    }

    // The receive tail: decode the INNER unidirectional payload (the frame_router owns the
    // frame_header strip and the type switch), resolve the fqn by topic_hash, and hand the
    // opaque wire_bytes up to on_message. A decode failure or unresolved topic_hash is
    // warn-and-DROPPED through the injected logger&: never thrown, never propagated.
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

        auto fqn = m_endpoint.registry().fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data);
    }

    // The metadata-bearing receive tail: same resolution as the 2-arg deliver, but it
    // finishes the message_info the session began at on_receive and fills
    // publication_sequence and (when the gid flag rode the frame) source_identity.
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

        auto fqn = m_endpoint.registry().fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        info.publication_sequence = decoded->header.sequence;
        if(decoded->endpoint_counter)
            info.source_identity = publisher_gid{source_node_id, *decoded->endpoint_counter};
        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data, info);
    }

    // The receive-path liveness stamp hook, wired by the engine to its liveliness
    // monitor's stamp_data; absent = no stamp, so the forwarder stays monitor-agnostic.
    void set_on_data_stamp(plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> hook)
    {
        m_on_data_stamp = std::move(hook);
    }

    // The egress-shed drop edge, wired by the engine to its posted drop_sink(): an egress
    // overflow ALSO emits a drop_event here (additively — the per-band counter still
    // moves). The sink posts, so the per-publish shed site never fires an observer inline.
    // Absent = counter-only (the forwarder stays observer-agnostic when unwired).
    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> hook)
    {
        m_on_drop = std::move(hook);
    }

    // The data-path observation sinks, wired by the engine to its posted fan-out (the
    // drop_sink precedent): publish fires the published sink ONCE after framing and the
    // delivered sink ONCE per fanned destination, both handing the framed buffer's view
    // (a shared addref of the single per-publish owner — no copy); an attach/detach verdict
    // fires the qos-change sink. Each sink posts, so a per-publish/per-destination fan never
    // touches an observer inline. Absent = one predictable branch (the forwarder stays
    // observer-agnostic when unwired).
    void on_published(plexus::detail::move_only_function<void(std::string_view, const message_view &)> hook)
    {
        m_on_published = std::move(hook);
    }

    void on_delivered(plexus::detail::move_only_function<
                      void(std::string_view, const message_info &, const message_view &)> hook)
    {
        m_on_delivered = std::move(hook);
    }

    void on_qos_change(plexus::detail::move_only_function<void(const qos_change_event &)> hook)
    {
        m_on_qos_change = std::move(hook);
    }

    // The consumer-paced PULL reply: replay up to min(max_samples, ring.count(),
    // k_fetch_cap) retained frames to the REQUESTING peer's channel ONLY. max_samples is
    // attacker-controlled, so the cap bounds the reply to owned ring frames.
    void fetch_latched(const peer &p, std::uint64_t topic_hash, std::uint32_t max_samples)
    {
        auto it = m_retained.find(topic_hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        const std::size_t limit =
            std::min<std::size_t>({static_cast<std::size_t>(max_samples), it->second.count(), k_fetch_cap});
        replay_window(p, it->second, limit);
    }

    // Resolve a topic_hash back to its fqn — the object-lane receive tail's analog of the
    // resolution the bytes path does inside deliver. Empty when the hash names no topic.
    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        return m_endpoint.registry().fqn_for(topic_hash);
    }

    // The per-(topic, band, cause) drop tally; 0 for an unknown topic or out-of-range band.
    [[nodiscard]] std::size_t dropped(std::string_view fqn, std::size_t band, detail::drop_cause cause) const
    {
        return m_endpoint.registry().dropped(wire::fqn_topic_hash(fqn), band, cause);
    }

    // The per-topic stamp-demand latch (for inspection — publish reads the record
    // directly). True when any subscriber wants message_info OR the topic is unknown (the
    // safe always-on default).
    [[nodiscard]] bool any_subscriber_wants_info(std::string_view fqn) const
    {
        return m_endpoint.registry().any_subscriber_wants_info(wire::fqn_topic_hash(fqn));
    }

private:
    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    // Frame [frame_header][unidirectional_header][counter?][payload] ONCE into a freshly
    // allocated owning buffer and return a wire_bytes whose view aliases it. The owner
    // (shared_ptr<const vector>) is the single per-publish allocation that the fan-out
    // shares by addref across every band slot + the latch ring — no per-destination copy.
    wire_bytes<> frame_owned(const wire::frame_header &fhdr, const wire::unidirectional_header &uhdr,
                             std::span<const std::byte> payload,
                             std::optional<std::uint64_t> counter)
    {
        auto buf = std::make_shared<std::vector<std::byte>>();
        wire::encode_unidirectional_frame_into(*buf, fhdr, uhdr, payload, counter);
        std::span<const std::byte> view{*buf};
        return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
    }

    // Push the just-framed frame into the per-topic KEEP_LAST-N history ring, sized once
    // from the declared depth (clamped to [1, k_history_depth_cap] so a hostile declare
    // cannot drive N x max_payload to OOM). The ring slots OWN their bytes; they copy
    // from the framed view (the publish's owner is released when the call returns).
    void retain_if_latched(std::uint64_t hash, const topic_qos &qos, const wire_bytes<> &framed)
    {
        if(!qos.latch)
            return;
        auto &ring = m_retained[hash];
        ring.resize_to(std::clamp<std::size_t>(qos.depth, 1, k_history_depth_cap));
        ring.push(static_cast<std::span<const std::byte>>(framed));
    }

    // Replay a latched topic's retained history to ONLY the new peer (not the fan-out
    // loop). The subscriber's stored durability choice gates it: `none` declines, `latest`
    // takes the newest frame, `all` takes the history oldest->newest capped to replay_depth.
    void replay_if_latched(const peer &p, std::uint64_t hash)
    {
        if(!m_endpoint.registry().qos_for(hash).latch)
            return;
        auto it = m_retained.find(hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        const subscriber_qos sub = m_endpoint.registry().qos_for_subscriber(hash, p.channel);
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

    // Send the most-recent `limit` frames of a ring to one peer, oldest->newest. Shared by
    // durability=all replay and the fetch_latched PULL reply.
    void replay_window(const peer &p, const detail::history_ring &ring, std::size_t limit)
    {
        const std::size_t count = ring.count();
        for(std::size_t i = count - limit; i < count; ++i)
            p.channel.send(ring.oldest_to_newest(i));
    }

    // The zero-serialization fast path is taken only on a same-process tier, over a
    // channel that exposes the object lane (if-constexpr on the concrete channel type, not
    // a concept verb), AND with a stored type_id matching the carrier's wire tag.
    static bool eligible_for_object(const typename subscriber_registry<channel_type>::subscriber &sub,
                                    const object_carrier &carrier)
    {
        if constexpr(requires(channel_type &c) { c.send_object(carrier); })
            return sub.tier == locality::process && sub.type_id && *sub.type_id == carrier.type_tag;
        else
            return false;
    }

    // A mismatch only when BOTH sides declared a type_id and they differ. Either side
    // undeclared (std::nullopt) is never a mismatch — absence is a distinct state, not a
    // zero type_id that would false-refuse.
    bool type_id_mismatch(std::uint64_t hash, std::optional<std::uint64_t> subscriber_type_id) const
    {
        const auto producer_type_id = m_endpoint.registry().producer_type_id(hash);
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
        // The type_id rides the type_hash field; 0 is the undeclared sentinel the producer
        // reads back as "no type declared" — never a mismatch.
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
        m_endpoint.send_subscribe(channel, req);
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

    // Forget a remembered topic on a genuine unsubscribe so the durable demand reflects
    // only live demand — a dropped topic must not be resurrected on reconnect.
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

    // Record the per-band shed counter (always on) AND, when the egress drop edge is
    // wired, emit the posted drop_event. The subscriber struct carries no peer node_id,
    // so the event is peer-less (node_id{}); tier is the subscriber's delivery locality.
    void shed(std::uint64_t hash, std::size_t band, detail::drop_cause cause, locality tier)
    {
        m_endpoint.registry().record_drop(hash, band, cause);
        if(m_on_drop)
            m_on_drop(detail::drop_event{.cause = cause, .transport = tier,
                                         .band = static_cast<std::uint8_t>(band), .topic_hash = hash});
    }

    // Hand the published/delivered view to the engine sink (which posts a snapshot to the
    // observer fan-out); absent = one branch. The view borrows the framed owner — the posted
    // closure on the engine side captures it by value (a shared addref), so the buffer
    // outlives the deferred turn (the owner-lifetime guarantee).
    void emit_published(std::string_view fqn, const message_view &view)
    {
        if(m_on_published)
            m_on_published(fqn, view);
    }

    void emit_delivered(std::string_view fqn, const message_info &info, const message_view &view)
    {
        if(m_on_delivered)
            m_on_delivered(fqn, info, view);
    }

    // Hand a resolved attach/detach QoS verdict to the engine sink. The subscriber struct
    // carries no peer node_id, so the event is peer-less (node_id{}) — consistent with the
    // peer-less drop_event the shed site emits.
    void emit_qos_change(qos_edge edge, std::uint64_t hash, const subscriber_qos &requested,
                         rxo_verdict verdict, std::optional<std::uint64_t> type_id)
    {
        if(m_on_qos_change)
            m_on_qos_change(qos_change_event{.edge = edge, .topic_hash = hash, .peer = node_id{},
                                             .requested = requested, .verdict = verdict,
                                             .type_id = type_id});
    }

    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger &m_logger;
    // The node-level per-message size default the RxO size relation resolves an offered
    // topic's 0=unset max against (effective_max). Set from the node-level default at
    // construction (no independent constant default here) so the forwarder and the data
    // path resolve against ONE value.
    std::size_t m_global_default;
    endpoint_type m_endpoint;
    detail::egress_scheduler<channel_type, Policy> m_egress;
    std::unordered_map<std::string, std::vector<remembered_demand>> m_remote_topics;
    std::unordered_map<std::uint64_t, detail::history_ring> m_retained;
    plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> m_on_data_stamp;
    plexus::detail::move_only_function<void(const detail::drop_event &)> m_on_drop;
    plexus::detail::move_only_function<void(std::string_view, const message_view &)> m_on_published;
    plexus::detail::move_only_function<
        void(std::string_view, const message_info &, const message_view &)> m_on_delivered;
    plexus::detail::move_only_function<void(const qos_change_event &)> m_on_qos_change;
};

}

#endif

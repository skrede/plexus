#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/detail/forwarder_fanout.h"
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

// Durable subscribe demand: stores the subscriber's OWN requested qos so a deferred dial
// resurrects the real periods (not a default 0). The fqn is the key.
struct remembered_demand
{
    std::string    fqn;
    subscriber_qos qos;
    // nullopt = undeclared — without it a typed subscriber surviving a reconnect would silently
    // re-subscribe untyped.
    std::optional<std::uint64_t> type_id;
};

// A publisher-declared depth is an attacker-controlled count and N x max_payload is a
// resource-exhaustion vector, so the ring capacity is clamped to [1, this].
constexpr std::size_t k_history_depth_cap = 1024;

// max_samples arrives from an unverified peer, so the reply is capped at
// min(max_samples, ring.count(), this) — never more than count owned ring frames.
constexpr std::size_t k_fetch_cap = 1024;

// Payload-opaque pub/sub engine. Refcount-gated (one subscribe per uninterrupted attach run),
// frames each publish ONCE and shares the single owning buffer across subscribers, and
// warn-and-drops a malformed frame on the receive tail through the injected logger&. It never
// interprets the payload.
//
// over-limit: one cohesive pub/sub engine; splitting scatters the shared registry + scratch state
// (the attach gate and the publish/object fan steps already extracted to detail/).
template<typename Policy>
    requires plexus::Policy<Policy>
class message_forwarder
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using endpoint_type = subscription_endpoint<channel_type>;
    using peer          = typename endpoint_type::peer;

    // The logger is a required, borrowed engine dependency. global_default is the node-level
    // per-message size ceiling the RxO size relation resolves an offered topic's 0=unset max against
    // — the SAME value the data-path transports use, so a remote subscribe is admitted against one
    // consistent ceiling, not a drift-prone local one.
    explicit message_forwarder(log::logger &logger,
                               std::size_t  global_default = io::global_default_max_message_bytes)
            : m_logger(logger)
            , m_global_default(global_default)
    {
    }

    // Per-(peer, fqn) refcount gate: only the 0->1 transition registers the fan-out entry and
    // emits the wire subscribe_request; later attaches just bump and return false.
    bool attach(const peer &p, std::string_view fqn, const subscriber_qos &qos = subscriber_qos{},
                std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, qos, type_id);
        record_remote_topic(p.node_name, fqn, qos, type_id);
        send_subscribe(p.channel, fqn, hash, type_id, qos);
        emit_qos_change(qos_edge::accepted, hash, qos, rxo_verdict::compatible, type_id);
        // The local node issued the subscribe — it is the SUBSCRIBER, so a co-host upgrade drains
        // the companion ring into the receive path.
        emit_demand_transition(p.node_name, fqn, demand_transition::up, demand_role::subscriber);
        return true;
    }

    // The producer-side reaction to an arriving subscribe (the full QoS gate); relocated to
    // detail::attach_for_fanout, which reads this forwarder's registry/endpoint through a friend
    // reference. A per-topic refusal replies a status and returns false, never a peer teardown.
    bool attach_for_fanout(const peer &p, std::string_view fqn,
                           std::optional<std::uint64_t> subscriber_type_id = std::nullopt,
                           const subscriber_qos        &sub_qos            = subscriber_qos{})
    {
        return detail::attach_for_fanout(*this, p, fqn, subscriber_type_id, sub_qos);
    }

    // Record durable subscribe demand WITHOUT a wire emit — possibly before the session exists
    // (async dial pending), so the session resurrects it through the counted path on completion.
    void remember_demand(const std::string &node_name, std::string_view fqn,
                         const subscriber_qos        &qos     = subscriber_qos{},
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

    // producer_type_id is the subscribe-time match authority. emit_source_identity opts the topic
    // into per-frame source-identity carriage (the gid flag + a varint endpoint counter the
    // receiver pairs with the session peer's node_id).
    void declare(std::string_view fqn, topic_qos qos,
                 std::optional<std::uint64_t> producer_type_id     = std::nullopt,
                 bool                         emit_source_identity = false)
    {
        m_endpoint.registry().declare(wire::fqn_topic_hash(fqn), fqn, qos, producer_type_id,
                                      emit_source_identity);
    }

    void latch(std::string_view fqn) { declare(fqn, topic_qos{.latch = true, .depth = 1}); }

    // Frame ONCE and fan the single owning buffer to each subscribed channel (no subscriber = no
    // send). session_id is per-send, not a member: a node-shared forwarder fans to many peers,
    // each with its own epoch.
    void publish(std::string_view fqn, std::span<const std::byte> payload,
                 std::uint64_t session_id = 0)
    {
        auto        hash  = wire::fqn_topic_hash(fqn);
        const auto *topic = m_endpoint.registry().entry_for(hash);
        const auto *subs =
                topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        // A null topic implies subs == nullptr and latched == false, so past this return
        // `topic` is always valid.
        if(subs == nullptr && !qos.latch)
            return;

        const std::optional<std::uint64_t> counter =
                topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
        message_info       pub_info{};
        const wire_bytes<> framed = frame_publish(hash, payload, counter, session_id, pub_info);
        // The BARE codec bytes (the framed buffer minus the frame prefix), an aliasing subspan
        // sharing the framed owner, so the offline projector never re-parses framing to strip it.
        const message_view bare = bare_of(framed, counter);
        emit_published(hash, fqn, bare);

        if(subs != nullptr)
            fan_published(*subs, hash, fqn, qos, framed, bare, pub_info);
        retain_if_latched(hash, qos, framed);
    }

    // The typed-encode step of publish: stamp the unidirectional + frame headers and frame ONCE
    // into an owning buffer (addref-shared across the fan + latch ring). The publication sequence
    // and source timestamp it stamps are returned in pub_info for the delivered observation.
    wire_bytes<> frame_publish(std::uint64_t hash, std::span<const std::byte> payload,
                               std::optional<std::uint64_t> counter, std::uint64_t session_id,
                               message_info &pub_info)
    {
        wire::unidirectional_header uhdr{.source     = wire::endpoint_source_type::publisher,
                                         .sequence   = m_endpoint.next_sequence(),
                                         .topic_hash = hash};
        wire::frame_header          fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                .session_id   = session_id,
                .timestamp_ns = wire::now_timestamp_ns(),
                .payload_len  = 0 // set inside the one-pass encode from the framed region size
        };
        pub_info.publication_sequence = uhdr.sequence;
        pub_info.source_timestamp     = fhdr.timestamp_ns;
        return frame_owned(fhdr, uhdr, payload, counter);
    }

    // The typed-encode step of publish_object: frame the just-encoded object bytes ONCE under the
    // carrier's pinned sequence + source timestamp (the lazy encode already ran).
    wire_bytes<> frame_object(const object_carrier &carrier, std::uint64_t hash,
                              std::optional<std::uint64_t> counter, std::uint64_t session_id,
                              std::span<const std::byte> bytes)
    {
        wire::unidirectional_header uhdr{.source     = wire::endpoint_source_type::publisher,
                                         .sequence   = carrier.sequence,
                                         .topic_hash = hash};
        wire::frame_header          fhdr{
                .type         = wire::msg_type::unidirectional,
                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                .session_id   = session_id,
                .timestamp_ns = carrier.source_timestamp,
                .payload_len  = 0 // set inside the one-pass encode from the framed region size
        };
        return frame_owned(fhdr, uhdr, bytes, counter);
    }

    // The confinement-gated fan of one framed publish to its subscribers: send only when the
    // topic's reach mask shares a bit with the subscriber's cached tier (fail-closed access
    // control), BEFORE the egress enqueue. The scheduler governs the LIVE fan-out only; control
    // frames and latch replay take direct send.
    void
    fan_published(const std::vector<typename subscriber_registry<channel_type>::subscriber> &subs,
                  std::uint64_t hash, std::string_view fqn, const topic_qos &qos,
                  const wire_bytes<> &framed, const message_view &bare,
                  const message_info &pub_info)
    {
        const std::size_t band = detail::band_of(qos.priority);
        for(const auto &sub : subs)
        {
            if(!any_set(qos.reach, sub.tier))
                continue;
            // The ONLY place a medium is chosen per message: the companion route returns the SHM
            // channel when this message fits the cap, else nullptr to keep the wire sub.channel
            // (the dual-delivery fail-safe). Gate on the FRAMED size, not the bare payload: the
            // ring slot carries framed bytes, so a bare-size gate would route a near-cap message
            // whose frame overflows the slot to SHM only, where the oversize send is dropped and
            // the wire fail-safe never runs.
            channel_type *route = sub.channel;
            if(m_companion_route)
                if(channel_type *companion = m_companion_route(sub.node_name, fqn, framed.size()))
                    route = companion;
            const detail::drop_cause cause = m_egress.enqueue(*route, band, qos.congestion, framed);
            if(cause != detail::drop_cause::none)
                shed(hash, band, cause, sub.tier);
            emit_delivered(hash, fqn, pub_info, bare);
        }
    }

    // The object-lane fan: a process-tier eligible subscriber takes the zero-serialization
    // send_object fast path (gated at COMPILE TIME — a channel without send_object never
    // instantiates the call); everyone else forces the lazy encode and rides the byte path.
    template<typename EncodeOnce>
    void fan_object(const std::vector<typename subscriber_registry<channel_type>::subscriber> &subs,
                    std::uint64_t hash, std::string_view fqn, const topic_qos &qos,
                    const object_carrier &carrier, const wire_bytes<> &framed,
                    const message_view &bare, const message_info &obj_info, EncodeOnce &encode_once)
    {
        const std::size_t band = detail::band_of(qos.priority);
        for(const auto &sub : subs)
        {
            if(!any_set(qos.reach, sub.tier))
                continue;
            if constexpr(requires(channel_type &c) { c.send_object(carrier); })
            {
                if(eligible_for_object(sub, carrier))
                {
                    sub.channel->send_object(carrier);
                    emit_delivered(hash, fqn, obj_info, bare);
                    continue;
                }
            }
            encode_once();
            const detail::drop_cause cause =
                    m_egress.enqueue(*sub.channel, band, qos.congestion, framed);
            if(cause != detail::drop_cause::none)
                shed(hash, band, cause, sub.tier);
            emit_delivered(hash, fqn, obj_info, bare);
        }
    }

    // The zero-serialization sibling of publish: fans a refcounted object handle behind the SAME
    // confinement gate, falling back to the byte path for any subscriber the fast path cannot
    // serve. encode is invoked AT MOST ONCE, lazily, the first time any byte need appears.
    //
    // Reference protocol: the CALLER owns one reference on entry; each fast-path send_object
    // addrefs through the bus, and this verb releases the caller's reference once after the fan
    // loop — so the slot is balanced on every path.
    template<typename EncodeFn>
    // NOLINTNEXTLINE(readability-function-size)
    void publish_object(std::string_view fqn, object_carrier carrier, EncodeFn &&encode,
                        std::uint64_t session_id = 0)
    {
        auto        hash  = wire::fqn_topic_hash(fqn);
        const auto *topic = m_endpoint.registry().entry_for(hash);
        const auto *subs =
                topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos     = topic != nullptr ? topic->qos : topic_qos{};
        const bool      latched = qos.latch;

        carrier.topic_hash = hash;
        carrier.sequence   = m_endpoint.next_sequence();
        // Skip the clock read when no subscriber wants message_info, leaving source_timestamp ==
        // 0 (the "not stamped" sentinel the ==0 keying relies on).
        if(carrier.source_timestamp == 0 && (topic == nullptr || topic->any_subscriber_wants_info))
            carrier.source_timestamp = wire::now_timestamp_ns();

        // Framed lazily AT MOST ONCE, then addref-shared across every byte-path subscriber + the
        // latch ring (frame-once-fan-to-N).
        wire_bytes<>                       framed;
        bool                               encoded = false;
        const std::optional<std::uint64_t> counter = topic != nullptr && topic->emit_source_identity
                ? topic->endpoint_counter
                : std::nullopt;
        const auto                         encode_once = [&]
        {
            if(encoded)
                return;
            encoded = true;
            framed  = frame_object(carrier, hash, counter, session_id,
                                   std::forward<EncodeFn>(encode)());
        };

        // The typed loan path carries NO payload, so its observation view is ENVELOPE-ONLY by
        // default — recording payload here is a sovereign opt-in. The capture gate decides BEFORE
        // the encode: only a selected, non-decimated, payload-fidelity topic forces the lazy
        // encode; everything else stays envelope-only and pays no encode.
        const bool capture_payload = m_capture_wants_payload && m_capture_wants_payload(hash);
        const message_info obj_info{.publication_sequence = carrier.sequence,
                                    .source_timestamp     = carrier.source_timestamp};
        if(capture_payload)
            encode_once();
        // Built only when the encode fired, so an unselected topic stays envelope-only and pays
        // no encode.
        const message_view bare = capture_payload ? bare_of(framed, counter) : message_view{};
        emit_published(hash, fqn, bare);

        if(subs != nullptr)
            fan_object(*subs, hash, fqn, qos, carrier, framed, bare, obj_info, encode_once);

        // Force one encode even when every live subscriber fast-pathed, so a late bytes joiner
        // replays a real frame from the history ring.
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
        emit_qos_change(qos_edge::unsubscribed, hash, subscriber_qos{}, rxo_verdict::compatible,
                        std::nullopt);
        // The down-edge drops whichever lane the pair held — the coordinator keys teardown by
        // (peer, fqn), not by role.
        emit_demand_transition(p.node_name, fqn, demand_transition::down, demand_role::subscriber);
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

    // A pure read so the forwarder stays readiness-agnostic: the session reads this to resurrect
    // its counted subscribes on reconnect. Empty vector for an unknown node_name.
    const std::vector<remembered_demand> &remembered_topics(const std::string &node_name) const
    {
        auto it = m_remote_topics.find(node_name);
        return it == m_remote_topics.end() ? m_empty : it->second;
    }

    // Decode the INNER unidirectional payload, resolve the fqn by topic_hash, and hand the opaque
    // wire_bytes up to on_message. A decode failure or unresolved topic_hash is warn-and-DROPPED
    // through the injected logger&: never thrown, never propagated.
    template<typename OnMessage>
    void deliver(const peer &p, std::span<const std::byte> inner, const node_id &source_node_id,
                 bool has_source_identity, OnMessage &&on_message)
    {
        (void)p; // identity symmetry with the procedure receive tail; resolution is by topic_hash
        // The bytes-only tail discards the counter but MUST still pass the flag: the producer
        // emits the varint per ITS topic declaration, so the data span is only correct when the
        // flag is honored on decode.
        auto decoded = wire::decode_unidirectional(inner, has_source_identity);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_endpoint.registry().fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data);
    }

    // Same resolution as the 2-arg deliver, finishing the message_info the session began at
    // on_receive (publication_sequence + source_identity when the gid flag rode the frame).
    //
    // DIRECT-DELIVERY INVARIANT (the gid rests on it): the gid's node_id half is the PINNED
    // session peer's node_id (source_node_id), NOT one taken from the frame. The forwarder never
    // relays across peer boundaries, so the wire carries only the endpoint counter and a peer can
    // claim counters ONLY within its own node_id namespace (structural anti-spoof). A future
    // relay/bridge topology would break this and MUST carry full origin identity on those frames.
    template<typename OnMessage>
    void deliver(const peer &p, std::span<const std::byte> inner, message_info info,
                 const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
        (void)p; // identity symmetry with the procedure receive tail; resolution is by topic_hash
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

    // Absent = no stamp, so the forwarder stays monitor-agnostic.
    void
    set_on_data_stamp(plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> hook)
    {
        m_on_data_stamp = std::move(hook);
    }

    // An egress overflow ALSO emits a drop_event here (additively — the per-band counter still
    // moves). The sink posts, so the shed site never fires an observer inline. Absent =
    // counter-only.
    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> hook)
    {
        m_on_drop = std::move(hook);
    }

    // The data-path observation sinks (the drop_sink precedent): publish fires published ONCE
    // after framing and delivered ONCE per fanned destination, both handing the framed view (a
    // shared addref, no copy). Each posts, so a fan never touches an observer inline. Absent =
    // one predictable branch.
    void on_published(plexus::detail::move_only_function<void(std::uint64_t, std::string_view,
                                                              const message_view &)>
                              hook)
    {
        m_on_published = std::move(hook);
    }

    void on_delivered(
            plexus::detail::move_only_function<void(std::uint64_t, std::string_view,
                                                    const message_info &, const message_view &)>
                    hook)
    {
        m_on_delivered = std::move(hook);
    }

    void on_qos_change(plexus::detail::move_only_function<void(const qos_change_event &)> hook)
    {
        m_on_qos_change = std::move(hook);
    }

    void set_capture_wants_payload(plexus::detail::move_only_function<bool(std::uint64_t)> hook)
    {
        m_capture_wants_payload = std::move(hook);
    }

    // The wire_fallback per-message companion route: given (peer node_name, fqn, message size) it
    // returns the SHM channel when the message fits the cap, else nullptr to keep the wire
    // sub.channel. Absent = the fan-out is byte-identical to a node with no SHM companion (the
    // fail-safe default: the wire is the standing channel, the ring an additive fast path).
    void on_companion_route(plexus::detail::move_only_function<
                            channel_type *(std::string_view, std::string_view, std::size_t)>
                                    hook)
    {
        m_companion_route = std::move(hook);
    }

    // The forwarder only ANNOUNCES the 0->1/1->0 edge it already knows from its refcount gate —
    // it gains no same_host, no acquire, no transport. Absent = one predictable branch.
    void
    on_demand_transition(plexus::detail::move_only_function<void(std::string_view, std::string_view,
                                                                 demand_transition, demand_role)>
                                 hook)
    {
        m_on_demand_transition = std::move(hook);
    }

    // The consumer-paced PULL reply: replay up to min(max_samples, ring.count(), k_fetch_cap)
    // retained frames to the REQUESTING peer ONLY (max_samples is attacker-controlled).
    void fetch_latched(const peer &p, std::uint64_t topic_hash, std::uint32_t max_samples)
    {
        auto it = m_retained.find(topic_hash);
        if(it == m_retained.end() || it->second.empty())
            return;
        const std::size_t limit = std::min<std::size_t>(
                {static_cast<std::size_t>(max_samples), it->second.count(), k_fetch_cap});
        replay_window(p, it->second, limit);
    }

    // Resolve a topic_hash back to its fqn (the object-lane analog of deliver's resolution).
    // Empty when the hash names no topic.
    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        return m_endpoint.registry().fqn_for(topic_hash);
    }

    // The topic's publisher-declared producer type_id, absent for an undeclared topic — the
    // recorder keys a captured sample to its declared schema by this id.
    [[nodiscard]] std::optional<std::uint64_t> producer_type_id(std::uint64_t topic_hash) const
    {
        return m_endpoint.registry().producer_type_id(topic_hash);
    }

    // The per-(topic, band, cause) drop tally; 0 for an unknown topic or out-of-range band.
    [[nodiscard]] std::size_t dropped(std::string_view fqn, std::size_t band,
                                      detail::drop_cause cause) const
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
    template<typename F, typename P>
    friend bool detail::attach_for_fanout(F &, const P &, std::string_view,
                                          std::optional<std::uint64_t>, const subscriber_qos &);

    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    // Frame [frame_header][unidirectional_header][counter?][payload] ONCE into an owning buffer.
    // The owner is the single per-publish allocation the fan-out shares by addref across every
    // band slot + the latch ring — no per-destination copy.
    wire_bytes<> frame_owned(const wire::frame_header          &fhdr,
                             const wire::unidirectional_header &uhdr,
                             std::span<const std::byte>         payload,
                             std::optional<std::uint64_t>       counter)
    {
        auto buf = std::make_shared<std::vector<std::byte>>();
        wire::encode_unidirectional_frame_into(*buf, fhdr, uhdr, payload, counter);
        std::span<const std::byte> view{*buf};
        return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
    }

    // An aliasing subspan past the frame prefix, sharing the framed owner. The prefix is the
    // framing layout the producer just wrote, so a payload-fidelity tap records bytes
    // byte-identical to encode().
    static message_view bare_of(const wire_bytes<> &framed, std::optional<std::uint64_t> counter)
    {
        const std::size_t prefix = wire::header_size + wire::unidirectional_header_size +
                (counter ? wire::varint_size(*counter) : 0);
        const std::span<const std::byte> view = static_cast<std::span<const std::byte>>(framed);
        return message_view{view.subspan(prefix), framed.owner()};
    }

    // Push into the per-topic KEEP_LAST-N history ring, sized from the declared depth (clamped to
    // [1, k_history_depth_cap] so a hostile declare cannot drive N x max_payload to OOM). The
    // ring slots OWN their bytes (the publish's owner is released when the call returns).
    void retain_if_latched(std::uint64_t hash, const topic_qos &qos, const wire_bytes<> &framed)
    {
        if(!qos.latch)
            return;
        auto &ring = m_retained[hash];
        ring.resize_to(std::clamp<std::size_t>(qos.depth, 1, k_history_depth_cap));
        ring.push(static_cast<std::span<const std::byte>>(framed));
    }

    // Replay a latched topic's history to ONLY the new peer. The subscriber's durability choice
    // gates it: none declines, latest takes the newest, all takes oldest->newest capped to
    // replay_depth.
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
            case durability::none:   return;
            case durability::latest: p.channel.send(it->second.newest()); return;
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

    // The fast path is taken only on a same-process tier, over a channel exposing the object lane,
    // AND with a stored type_id matching the carrier's wire tag.
    static bool
    eligible_for_object(const typename subscriber_registry<channel_type>::subscriber &sub,
                        const object_carrier                                         &carrier)
    {
        if constexpr(requires(channel_type &c) { c.send_object(carrier); })
            return sub.tier == locality::process && sub.type_id && *sub.type_id == carrier.type_tag;
        else
            return false;
    }

    // A mismatch only when BOTH sides declared a type_id and they differ — either side undeclared
    // is never a mismatch (absence is a distinct state, not a false-refusing zero).
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
                        const subscriber_qos        &sub_qos = subscriber_qos{})
    {
        // 0 is the undeclared sentinel the producer reads back as "no type declared".
        wire::subscribe_request req{.fqn        = std::string{fqn},
                                    .type_name  = {},
                                    .topic_hash = hash,
                                    .type_hash  = type_id.value_or(0),
                                    .source     = wire::endpoint_source_type::publisher};
        // Carry the choice OUT only when it differs from the default, so a default subscribe
        // stays byte-identical to the pre-region encoding.
        if(!(sub_qos == subscriber_qos{}))
        {
            req.has_qos = true;
            req.qos     = to_wire_region(sub_qos);
        }
        m_endpoint.send_subscribe(channel, req);
    }

    void record_remote_topic(const std::string &node_name, std::string_view fqn,
                             const subscriber_qos        &qos,
                             std::optional<std::uint64_t> type_id = std::nullopt)
    {
        auto &topics = m_remote_topics[node_name];
        for(const auto &existing : topics)
            if(existing.fqn == fqn)
                return; // idempotent: a re-record keeps the first stored qos + type_id
        topics.emplace_back(remembered_demand{std::string{fqn}, qos, type_id});
    }

    // Forget on a genuine unsubscribe so a dropped topic is not resurrected on reconnect.
    void forget_remote_topic(const std::string &node_name, std::string_view fqn)
    {
        auto it = m_remote_topics.find(node_name);
        if(it == m_remote_topics.end())
            return;
        std::erase_if(it->second, [&](const remembered_demand &d) { return d.fqn == fqn; });
        if(it->second.empty())
            m_remote_topics.erase(it);
    }

    // A plain store on the receive path — no timer. No-op until the engine wires the monitor.
    void stamp_received(const node_id &source_node_id, std::uint64_t topic_hash)
    {
        if(m_on_data_stamp)
            m_on_data_stamp(source_node_id, topic_hash);
    }

    // Record the per-band counter (always on) AND, when wired, emit the posted drop_event. The
    // subscriber struct carries no peer node_id, so the event is peer-less (node_id{}).
    void shed(std::uint64_t hash, std::size_t band, detail::drop_cause cause, locality tier)
    {
        m_endpoint.registry().record_drop(hash, band, cause);
        if(m_on_drop)
            m_on_drop(detail::drop_event{.cause      = cause,
                                         .transport  = tier,
                                         .band       = static_cast<std::uint8_t>(band),
                                         .topic_hash = hash});
    }

    // The view borrows the framed owner; the engine-side posted closure addrefs it, so the buffer
    // outlives the deferred turn. Absent = one branch.
    void emit_published(std::uint64_t hash, std::string_view fqn, const message_view &view)
    {
        if(m_on_published)
            m_on_published(hash, fqn, view);
    }

    void emit_demand_transition(std::string_view node_name, std::string_view fqn,
                                demand_transition dir, demand_role role)
    {
        if(m_on_demand_transition)
            m_on_demand_transition(node_name, fqn, dir, role);
    }

    void emit_delivered(std::uint64_t hash, std::string_view fqn, const message_info &info,
                        const message_view &view)
    {
        if(m_on_delivered)
            m_on_delivered(hash, fqn, info, view);
    }

    // The subscriber struct carries no peer node_id, so the event is peer-less (node_id{}) —
    // consistent with the peer-less drop_event the shed site emits.
    void emit_qos_change(qos_edge edge, std::uint64_t hash, const subscriber_qos &requested,
                         rxo_verdict verdict, std::optional<std::uint64_t> type_id)
    {
        if(m_on_qos_change)
            m_on_qos_change(qos_change_event{.edge       = edge,
                                             .topic_hash = hash,
                                             .peer       = node_id{},
                                             .requested  = requested,
                                             .verdict    = verdict,
                                             .type_id    = type_id});
    }

    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger &m_logger;
    // The node-level per-message size ceiling the RxO size relation resolves an offered topic's
    // 0=unset max against, so the forwarder and the data path resolve against ONE value.
    std::size_t                                                              m_global_default;
    endpoint_type                                                            m_endpoint;
    detail::egress_scheduler<channel_type, Policy>                           m_egress;
    std::unordered_map<std::string, std::vector<remembered_demand>>          m_remote_topics;
    // The forwarder owns this never-mutated empty vector so remembered_topics returns it by
    // reference on the unknown-node branch — no function-local static, no dynamic-init guard.
    std::vector<remembered_demand>                                           m_empty;
    std::unordered_map<std::uint64_t, detail::history_ring>                  m_retained;
    plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> m_on_data_stamp;
    plexus::detail::move_only_function<void(const detail::drop_event &)>     m_on_drop;
    plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_view &)>
            m_on_published;
    plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_info &,
                                            const message_view &)>
                                                                       m_on_delivered;
    plexus::detail::move_only_function<void(const qos_change_event &)> m_on_qos_change;
    // Returns true when the topic wants a payload encode this record (selected at payload fidelity
    // AND admitted by decimation). Unset on a node with no capture policy (one null-check, never
    // encodes).
    plexus::detail::move_only_function<bool(std::uint64_t)> m_capture_wants_payload;
    // size <= cap -> the SHM channel, else nullptr -> the wire sub.channel. Unset on every node
    // with no SHM wire_fallback companion (the fan-out branch is never entered there).
    plexus::detail::move_only_function<channel_type *(std::string_view, std::string_view,
                                                      std::size_t)>
            m_companion_route;
    // The coordinator's on_edge. Unset on a node with no same-host upgrade coordinator (one
    // predictable branch at the attach/detach gate).
    plexus::detail::move_only_function<void(std::string_view, std::string_view, demand_transition,
                                            demand_role)>
            m_on_demand_transition;
};

}

#endif

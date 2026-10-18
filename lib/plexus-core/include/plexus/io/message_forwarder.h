#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/policy.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include "plexus/graph/topic_record.h"

#include "plexus/io/qos_rxo.h"
#include "plexus/io/locality.h"
#include "plexus/io/message_info.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/history_limits.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/forward_options.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/remembered_demand.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/forward_gate.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/detail/forwarder_demand.h"
#include "plexus/io/detail/forwarder_fanout.h"
#include "plexus/io/detail/forwarder_deliver.h"
#include "plexus/io/detail/forwarder_publish.h"
#include "plexus/io/detail/forwarder_companion.h"
#include "plexus/io/detail/forwarder_subscribe.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/forwarded_frame.h"
#include "plexus/wire/topic_declaration.h"

#include <set>
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

template<typename Policy>
    requires plexus::Policy<Policy>
class message_forwarder
{
public:
    using channel_type  = typename Policy::byte_channel_type;
    using endpoint_type = subscription_endpoint<channel_type>;
    using peer          = typename endpoint_type::peer;

    explicit message_forwarder(log::logger &logger, std::size_t global_default = io::global_default_max_message_bytes)
            : m_logger(logger)
            , m_global_default(global_default)
    {
    }

    bool attach(const peer &p, std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt,
                std::string_view type_name = {})
    {
        if(!m_endpoint.subscribe_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().note_topic(hash, fqn);
        detail::record_remote_topic(*this, p.node_name, fqn, qos, type_id, type_name);
        emit_demand_transition(p.node_name, fqn, demand_transition::up, demand_role::subscriber);
        detail::send_subscribe(*this, p.channel, fqn, hash, type_id, qos, type_name);
        emit_qos_change(qos_edge::accepted, hash, qos, rxo_verdict::compatible, type_id);
        return true;
    }

    bool attach_for_fanout(const peer &p, std::string_view fqn, std::optional<std::uint64_t> subscriber_type_id = std::nullopt, const subscriber_qos &sub_qos = subscriber_qos{})
    {
        return detail::attach_for_fanout(*this, p, fqn, subscriber_type_id, sub_qos);
    }

    bool detach_for_fanout(const peer &p, std::string_view fqn)
    {
        return detail::forwarder_detach_for_fanout(*this, p, fqn);
    }

    // Register a same-node self-route as a first-class subscriber: registry add only — no wire
    // subscribe, no remembered demand, no demand transition (the self-channel never dials and a
    // local subscription creates no wire demand). Idempotent via the registry's dedup by channel.
    void attach_local(std::string_view fqn, channel_type &channel, std::string_view self_name, const subscriber_qos &qos = subscriber_qos{},
                      std::optional<std::uint64_t> type_id = std::nullopt)
    {
        const auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, channel, self_name, qos, type_id);
        // Record the self-route by its channel POINTER (a proven registration, not the spoofable
        // self_name tag) so the origination gate never wraps a 0x0F onto a same-node self-channel.
        ++m_self_route_channels[static_cast<const void *>(&channel)];
        detail::replay_if_latched(*this, peer{channel, std::string{self_name}}, hash);
    }

    void detach_local(std::string_view fqn, channel_type &channel)
    {
        detail::forwarder_detach_local(*this, fqn, channel);
    }

    void remember_demand(const std::string &node_name, std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt,
                         std::string_view type_name = {})
    {
        detail::record_remote_topic(*this, node_name, fqn, qos, type_id, type_name);
    }

    void forget_remembered_demand(const std::string &node_name, std::string_view fqn)
    {
        detail::forget_remote_topic(*this, node_name, fqn);
    }

    void declare(std::string_view fqn, topic_qos qos, std::optional<std::uint64_t> producer_type_id = std::nullopt, bool emit_source_identity = false,
                 std::string_view producer_type_name = {}, std::uint64_t schema_hint = 0)
    {
        const auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().declare(hash, fqn, qos, producer_type_id, emit_source_identity, producer_type_name, schema_hint);
        announce_declaration(remember_declaration(fqn, hash, producer_type_id, producer_type_name));
    }

    // Every topic this node declared, in the shape a peer is told about it. The replay a session
    // runs on connect reads this, not the registry: the registry's producer_type_name borrows the
    // publisher's option storage, and these copies outlive it.
    template<typename Fn>
    void for_each_local_declaration(Fn fn) const
    {
        for(const auto &declaration : m_local_declarations)
            fn(declaration);
    }

    void send_declare(channel_type &channel, const wire::topic_declaration &td)
    {
        m_endpoint.send_declare(channel, td);
    }

    void send_peer_report(channel_type &channel, std::span<const std::byte> bytes)
    {
        m_endpoint.send_peer_report(channel, bytes);
    }

    // The interest-scoped forwarding-send: fan a relay's pre-built outbound forwarded envelope to the
    // topic's actual subscribers, excluding the arrival session AND any same-node self-route (both loop
    // guards — a self-route would decode framed bytes as unidirectional), through the SAME per-destination
    // egress scheduler the publish path uses — one slow leg drops-with-count in its own band while an
    // unrelated destination drains unaffected (no head-of-line blocking). The envelope is built once on
    // the first eligible destination and addref-shared into every band (frame-once-fan-to-N); a build that
    // could not fit a slot (oversize) or found no free slot (exhaustion) drops-with-count per destination.
    template<typename BuildOnce>
    // NOLINTNEXTLINE(readability-function-size)
    void fan_forwarded_buffer(std::uint64_t hash, const void *arrival, BuildOnce &&build_once)
    {
        const auto *topic = m_endpoint.registry().entry_for(hash);
        if(topic == nullptr || topic->subscribers.empty())
            return;

        const topic_qos qos    = topic->qos;
        const std::size_t band = detail::band_of(qos.priority);
        for(const auto &sub : topic->subscribers)
        {
            const void *ch = static_cast<const void *>(&sub.channel.get());
            if(ch == arrival || m_self_route_channels.contains(ch) || !any_set(qos.reach, sub.tier))
                continue;
            const auto &built = build_once();
            if(built.bytes.empty()) // oversize / pool exhaustion: count the affected destination, enqueue nothing
            {
                if(built.cause != detail::drop_cause::none)
                    shed(hash, band, built.cause, sub.tier);
                continue;
            }
            // Splice-time envelope gate: a frame past the outbound leg's ceiling drops-with-count here
            // rather than being sent and tearing the narrow session down (unprobed channel = unlimited).
            if(built.bytes.size() > detail::channel_frame_ceiling(sub.channel.get()))
            {
                shed(hash, band, detail::drop_cause::splice_oversize, sub.tier);
                continue;
            }
            const detail::drop_cause cause = m_egress.enqueue(sub.channel.get(), band, qos.congestion, built.bytes);
            if(cause != detail::drop_cause::none)
                shed(hash, band, cause, sub.tier);
        }
    }

    // Relay-installed only: the seam a completed forwarded receive fires so the relay twin re-fans the
    // frame. A non-relay node never installs it, so the forwarded receive path stays a pure local
    // delivery with no splice code reached.
    void on_forward_refan(plexus::detail::move_only_function<void(std::uint64_t, const node_id &, std::uint8_t, std::span<const std::byte>, const channel_type *, const wire::shared_bytes *)> hook)
    {
        m_on_forward_refan_cb = std::move(hook);
    }

    bool wants_refan() const noexcept
    {
        return static_cast<bool>(m_on_forward_refan_cb);
    }

    void refan_forwarded(std::uint64_t hash, const node_id &origin, std::uint8_t hop, std::span<const std::byte> inner, const channel_type *arrival, const wire::shared_bytes *owner)
    {
        if(m_on_forward_refan_cb)
            m_on_forward_refan_cb(hash, origin, hop, inner, arrival, owner);
    }

    // True iff a subscriber for the topic exists whose channel is NEITHER the arrival session NOR a local
    // self-route: the non-local-demand gate the pub/sub origination fires on. A self-route is excluded by
    // the channel pointer recorded at attach_local time, so a purely-locally-demanded topic never
    // originates a 0x0F envelope onto its own self-channel (which decodes framed bytes as unidirectional).
    bool has_remote_demand(std::uint64_t hash, const void *arrival) const
    {
        const auto *topic = m_endpoint.registry().entry_for(hash);
        if(topic == nullptr)
            return false;
        for(const auto &sub : topic->subscribers)
        {
            const void *ch = static_cast<const void *>(&sub.channel.get());
            if(ch != arrival && !m_self_route_channels.contains(ch))
                return true;
        }
        return false;
    }

    // Relay-installed only: the seam a plain-unidirectional receive fires so the relay re-originates a
    // directly-attached publisher's publish as a forwarded envelope (the pub/sub analog of the
    // call -> forward_call fallback). The engine installs the scope + non-local-demand gate; a non-relay
    // node installs nothing, so wants_refan() short-circuits the receive path before this is reached.
    void on_originate(plexus::detail::move_only_function<void(std::uint64_t, const node_id &, std::span<const std::byte>, const channel_type *)> hook)
    {
        m_on_originate_cb = std::move(hook);
    }

    void originate_forwarded(std::uint64_t hash, const node_id &origin, std::span<const std::byte> header_on_inner, const channel_type *arrival)
    {
        if(m_on_originate_cb)
            m_on_originate_cb(hash, origin, header_on_inner, arrival);
    }

    // The by-destination relay seam a transiting request/response inner frame re-resolves through: the
    // engine route_selects over the destination's candidates and re-wraps onto the chosen via-session,
    // holding no per-correlation state. Installed on every engine (a request/response leg is not
    // relay-profile-gated); a forwarder with no engine attached leaves it null and never re-forwards.
    void on_forward_rpc(plexus::detail::move_only_function<bool(const node_id &, const node_id &, std::uint8_t, std::span<const std::byte>)> hook)
    {
        m_on_forward_rpc_cb = std::move(hook);
    }

    bool forward_rpc_installed() const noexcept
    {
        return static_cast<bool>(m_on_forward_rpc_cb);
    }

    bool forward_rpc(const node_id &origin, const node_id &destination, std::uint8_t hop, std::span<const std::byte> inner_frame)
    {
        return m_on_forward_rpc_cb && m_on_forward_rpc_cb(origin, destination, hop, inner_frame);
    }

    void on_declaration(plexus::detail::move_only_function<void(const wire::topic_declaration &)> hook)
    {
        m_on_declaration_cb = std::move(hook);
    }

    void latch(std::string_view fqn)
    {
        declare(fqn, topic_qos{.latch = true, .depth = 1});
    }

    void publish(std::string_view fqn, std::span<const std::byte> payload, std::uint64_t session_id = 0)
    {
        detail::forwarder_publish(*this, fqn, payload, session_id);
    }

    template<typename EncodeOnce>
    void fan_object(const std::vector<typename subscriber_registry<channel_type>::subscriber> &subs, std::uint64_t hash, std::string_view fqn, const topic_qos &qos,
                    const object_carrier &carrier, const wire_bytes<> &framed, const message_view &bare, const message_info &obj_info, EncodeOnce &encode_once)
    {
        const std::size_t band = detail::band_of(qos.priority);
        for(const auto &sub : subs)
        {
            if(!any_set(qos.reach, sub.tier))
                continue;

            if constexpr(requires(channel_type &c) { c.send_object(carrier); })
                if(eligible_for_object(sub, carrier))
                {
                    sub.channel.get().send_object(carrier);
                    emit_delivered(hash, fqn, obj_info, bare);
                    continue;
                }

            encode_once();
            const detail::drop_cause cause = m_egress.enqueue(sub.channel.get(), band, qos.congestion, framed);
            if(cause != detail::drop_cause::none)
                shed(hash, band, cause, sub.tier);

            emit_delivered(hash, fqn, obj_info, bare);
        }
    }

    template<typename EncodeFn>
    // NOLINTNEXTLINE(readability-function-size)
    void publish_object(std::string_view fqn, object_carrier carrier, EncodeFn &&encode, std::uint64_t session_id = 0)
    {
        auto hash           = wire::fqn_topic_hash(fqn);
        const auto *topic   = m_endpoint.registry().entry_for(hash);
        const auto *subs    = topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        const bool latched  = qos.latch;

        carrier.topic_hash = hash;
        carrier.sequence   = m_endpoint.next_sequence();
        if(carrier.source_timestamp == 0 && (topic == nullptr || topic->any_subscriber_wants_info))
            carrier.source_timestamp = wire::now_timestamp_ns();

        wire_bytes<> framed;
        bool encoded                               = false;
        const std::optional<std::uint64_t> counter = topic != nullptr && topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
        const auto encode_once                     = [&]
        {
            if(encoded)
                return;
            encoded = true;
            framed  = detail::frame_object(carrier, hash, counter, session_id, std::forward<EncodeFn>(encode)());
        };

        const bool capture_payload = m_capture_wants_payload_cb && m_capture_wants_payload_cb(hash);
        const message_info obj_info{.publication_sequence = carrier.sequence, .source_timestamp = carrier.source_timestamp};
        if(capture_payload)
            encode_once();

        const message_view bare = capture_payload ? bare_of(framed, counter) : message_view{};
        emit_published(hash, fqn, bare);

        if(subs != nullptr)
            fan_object(*subs, hash, fqn, qos, carrier, framed, bare, obj_info, encode_once);

        if(latched)
        {
            encode_once();
            detail::retain_if_latched(*this, hash, qos, framed);
        }

        release(carrier);
    }

    bool detach(const peer &p, std::string_view fqn)
    {
        return detail::forwarder_detach(*this, p, fqn);
    }

    void detach_all(const peer &p)
    {
        detail::forwarder_detach_all(*this, p);
    }

    const std::vector<remembered_demand> &remembered_topics(const std::string &node_name) const
    {
        return detail::remembered_topics(*this, node_name);
    }

    template<typename OnMessage>
    void deliver(const peer &, std::span<const std::byte> inner, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
        detail::forwarder_deliver(*this, inner, source_node_id, has_source_identity, std::forward<OnMessage>(on_message));
    }

    template<typename OnMessage>
    void deliver(const peer &, std::span<const std::byte> inner, message_info info, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
        detail::forwarder_deliver(*this, inner, info, source_node_id, has_source_identity, std::forward<OnMessage>(on_message));
    }

    void set_on_data_stamp(plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> hook)
    {
        m_on_data_stamp_cb = std::move(hook);
    }

    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> hook)
    {
        m_on_drop_cb = std::move(hook);
    }

    void on_published(plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_view &)> hook)
    {
        m_on_published_cb = std::move(hook);
    }

    void on_delivered(plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_info &, const message_view &)> hook)
    {
        m_on_delivered_cb = std::move(hook);
    }

    void on_qos_change(plexus::detail::move_only_function<void(const qos_change_event &)> hook)
    {
        m_on_qos_change_cb = std::move(hook);
    }

    void set_capture_wants_payload(plexus::detail::move_only_function<bool(std::uint64_t)> hook)
    {
        m_capture_wants_payload_cb = std::move(hook);
    }

    void on_companion_route(plexus::detail::move_only_function<channel_type *(std::string_view, std::string_view, std::size_t)> hook)
    {
        m_companion_route_cb = std::move(hook);
    }

    void on_companion(detail::companion_hooks hooks)
    {
        m_companion_hooks = std::move(hooks);
    }

    void note_companion(const peer &p, std::uint64_t topic_hash, companion_event event)
    {
        if(m_companion_hooks.event_cb)
            m_companion_hooks.event_cb(p.node_name, fqn_for(topic_hash), event);
    }

    void send_companion(const peer &p, std::string_view fqn, companion_event event)
    {
        detail::send_companion(m_endpoint, p.channel, fqn, event);
    }

    void on_demand_transition(plexus::detail::move_only_function<void(std::string_view, std::string_view, demand_transition, demand_role)> hook)
    {
        m_on_demand_transition_cb = std::move(hook);
    }

    void on_topic_edge(plexus::detail::move_only_function<void(const graph::topic_edge &)> hook)
    {
        m_on_topic_edge_cb = std::move(hook);
    }

    // What a peer told us about a topic. The views borrow the decoded frame, so the sink must copy
    // anything it keeps — the transport recycles that buffer the moment the fold returns.
    void note_topic_edge(const graph::topic_edge &edge)
    {
        if(m_on_topic_edge_cb)
            m_on_topic_edge_cb(edge);
    }

    void on_peer_report(plexus::detail::move_only_function<void(const node_id &, const wire::peer_report &)> hook)
    {
        m_on_peer_report_cb = std::move(hook);
    }

    // A relay's re-announcement of a THIRD-party origin, forwarded to the engine gate chain. The
    // report borrows the decoded frame, so the sink copies only what the owning route table keeps.
    void note_peer_report(const node_id &reporter, const wire::peer_report &pr)
    {
        if(m_on_peer_report_cb)
            m_on_peer_report_cb(reporter, pr);
    }

    // The node-wide forwarded-frame admission verdict, run against the shared per-(origin, arrival-relay)
    // dedup state so a duplicate never double-delivers regardless of which session carried it. Pure — the
    // gate mutates the dedup window only on an admit; the caller delivers only on admit.
    detail::forward_admission admit_forwarded(const wire::forwarded_frame &ff, const node_id &local_id, const node_id &arrival_relay)
    {
        return detail::forward_gate(ff, local_id, arrival_relay, m_forward_ctx.hop_budget, m_forward_ctx.dedup_depth, m_forward_dedup);
    }

    void set_forward_options(const forward_options &opts)
    {
        m_forward_ctx = make_forward_ctx(opts);
    }

    // Whether this node consumes a relayed publish addressed to it. A node that never uses relayed
    // routes still admits a forwarded frame through the shared dedup gate (so a relay re-fanning it
    // stays sequenced) but never delivers it to its own subscribers — the receive-side half of the
    // never posture, the send/reply half living at the forward_rpc and call-fallback selection sites.
    // The per-origin overload additionally retires relayed delivery for a single origin because a live
    // direct session to that origin supersedes the relayed one.
    void set_consume_relayed(bool consume) noexcept
    {
        m_consume_relayed = consume;
    }

    bool consumes_relayed() const noexcept
    {
        return m_consume_relayed;
    }

    // Cold path (direct-session lifecycle only): retire / restore relayed delivery for one origin whose
    // direct session supersedes / has lost the relayed path. A suppressed origin's frame is still admitted
    // through the shared dedup window and only dropped at local delivery, so the window stays sequenced and
    // resume delivers the next frame with no gap.
    void suppress_relayed_from(const node_id &origin)
    {
        m_relayed_suppressed.insert(origin);
    }

    void resume_relayed_from(const node_id &origin)
    {
        m_relayed_suppressed.erase(origin);
    }

    // The data-plane counterpart to the control-plane window re-arm: a relay returning under the same
    // identity restarts its forwarding splice sequence at 0 while this node's per-(origin, relay) dedup
    // window is still anchored high from the dead incarnation. Drop that window so the first re-forwarded
    // frame re-anchors and delivery resumes contiguously instead of dropping as too_old.
    void reset_forward_dedup(const node_id &origin, const node_id &arrival_relay)
    {
        m_forward_dedup.reset(origin, arrival_relay);
    }

    bool consumes_relayed_from(const node_id &origin) const noexcept
    {
        return m_consume_relayed && m_relayed_suppressed.find(origin) == m_relayed_suppressed.end();
    }

    void fetch_latched(const peer &p, std::uint64_t topic_hash, std::uint32_t max_samples)
    {
        auto it = m_retained.find(topic_hash);
        if(it == m_retained.end() || it->second.empty())
            return;

        const auto limit = std::min<std::size_t>({static_cast<std::size_t>(max_samples), it->second.count(), k_fetch_cap});
        detail::replay_window(p, it->second, limit);
    }

    std::string_view fqn_for(std::uint64_t topic_hash) const
    {
        return m_endpoint.registry().fqn_for(topic_hash);
    }

    std::optional<std::uint64_t> producer_type_id(std::uint64_t topic_hash) const
    {
        return m_endpoint.registry().producer_type_id(topic_hash);
    }

    std::size_t dropped(std::string_view fqn, std::size_t band, detail::drop_cause cause) const
    {
        return m_endpoint.registry().dropped(wire::fqn_topic_hash(fqn), band, cause);
    }

    bool any_subscriber_wants_info(std::string_view fqn) const
    {
        return m_endpoint.registry().any_subscriber_wants_info(wire::fqn_topic_hash(fqn));
    }

private:
    template<typename F, typename P>
    friend bool detail::attach_for_fanout(F &, const P &, std::string_view, std::optional<std::uint64_t>, const subscriber_qos &);
    template<typename F, typename C>
    friend void detail::send_subscribe(F &, C &, std::string_view, std::uint64_t, std::optional<std::uint64_t>, const subscriber_qos &, std::string_view);
    template<typename F>
    friend void detail::forwarder_drop(F &, std::string_view);
    template<typename F>
    friend void detail::stamp_received(F &, const node_id &, std::uint64_t);
    template<typename F, typename O>
    friend void detail::forwarder_deliver(F &, std::span<const std::byte>, const node_id &, bool, O &&);
    template<typename F, typename O>
    friend void detail::forwarder_deliver(F &, std::span<const std::byte>, message_info, const node_id &, bool, O &&);
    template<typename F>
    friend wire_bytes<> detail::frame_publish(F &, std::uint64_t, std::span<const std::byte>, std::optional<std::uint64_t>, std::uint64_t, message_info &);
    template<typename F>
    friend void detail::retain_if_latched(F &, std::uint64_t, const topic_qos &, const wire_bytes<> &);
    template<typename F, typename P>
    friend void detail::replay_if_latched(F &, const P &, std::uint64_t);
    template<typename F, typename S>
    friend void detail::fan_published(F &, const S &, std::uint64_t, std::string_view, const topic_qos &, const wire_bytes<> &, const message_view &, const message_info &);
    template<typename F>
    friend void detail::forwarder_publish(F &, std::string_view, std::span<const std::byte>, std::uint64_t);
    template<typename F>
    friend void detail::record_remote_topic(F &, const std::string &, std::string_view, const subscriber_qos &, std::optional<std::uint64_t>, std::string_view);
    template<typename F>
    friend void detail::forget_remote_topic(F &, const std::string &, std::string_view);
    template<typename F>
    friend const std::vector<remembered_demand> &detail::remembered_topics(const F &, const std::string &);
    template<typename F, typename P>
    friend bool detail::forwarder_detach(F &, const P &, std::string_view);
    template<typename F, typename P>
    friend bool detail::forwarder_detach_for_fanout(F &, const P &, std::string_view);
    template<typename F, typename P>
    friend void detail::forwarder_detach_all(F &, const P &);
    template<typename F, typename C>
    friend void detail::forwarder_detach_local(F &, std::string_view, C &);

    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    static message_view bare_of(const wire_bytes<> &framed, std::optional<std::uint64_t> counter)
    {
        const std::size_t prefix              = wire::header_size + wire::unidirectional_header_size + (counter ? wire::varint_size(*counter) : 0);
        const std::span<const std::byte> view = static_cast<std::span<const std::byte>>(framed);
        return message_view{view.subspan(prefix), framed.owner()};
    }

    static bool eligible_for_object(const typename subscriber_registry<channel_type>::subscriber &sub, const object_carrier &carrier)
    {
        if constexpr(requires(channel_type &c) { c.send_object(carrier); })
            return sub.tier == locality::process && sub.type_id && *sub.type_id == carrier.type_tag;
        else
            return false;
    }

    bool type_id_mismatch(std::uint64_t hash, std::optional<std::uint64_t> subscriber_type_id) const
    {
        const auto producer_type_id = m_endpoint.registry().producer_type_id(hash);
        if(!producer_type_id || !subscriber_type_id)
            return false;
        return *producer_type_id != *subscriber_type_id;
    }

    // Idempotent per topic: a re-declare replaces the stored assertion rather than stacking a
    // second one, so the replay emits one declaration per declared topic.
    const wire::topic_declaration &remember_declaration(std::string_view fqn, std::uint64_t hash, std::optional<std::uint64_t> type_id, std::string_view type_name)
    {
        wire::topic_declaration td{hash, type_id.value_or(0), std::string{fqn}, std::string{type_name}, detail::state_of(type_id, type_name)};
        for(auto &existing : m_local_declarations)
            if(existing.topic_hash == hash)
                return existing = std::move(td);
        return m_local_declarations.emplace_back(std::move(td));
    }

    void announce_declaration(const wire::topic_declaration &td)
    {
        if(m_on_declaration_cb)
            m_on_declaration_cb(td);
    }

    void shed(std::uint64_t hash, std::size_t band, detail::drop_cause cause, locality tier)
    {
        m_endpoint.registry().record_drop(hash, band, cause);
        if(m_on_drop_cb)
            m_on_drop_cb(detail::drop_event{.cause = cause, .transport = tier, .band = static_cast<std::uint8_t>(band), .topic_hash = hash});
    }

    void emit_published(std::uint64_t hash, std::string_view fqn, const message_view &view)
    {
        if(m_on_published_cb)
            m_on_published_cb(hash, fqn, view);
    }

    void emit_demand_transition(std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
    {
        if(m_on_demand_transition_cb)
            m_on_demand_transition_cb(node_name, fqn, dir, role);
    }

    void emit_delivered(std::uint64_t hash, std::string_view fqn, const message_info &info, const message_view &view)
    {
        if(m_on_delivered_cb)
            m_on_delivered_cb(hash, fqn, info, view);
    }

    void emit_qos_change(qos_edge edge, std::uint64_t hash, const subscriber_qos &requested, rxo_verdict verdict, std::optional<std::uint64_t> type_id)
    {
        if(m_on_qos_change_cb)
            m_on_qos_change_cb(qos_change_event{.edge = edge, .topic_hash = hash, .peer = node_id{}, .requested = requested, .verdict = verdict, .type_id = type_id});
    }

    log::logger &m_logger;
    std::size_t m_global_default;
    forward_ctx m_forward_ctx;
    bool m_consume_relayed{true};
    std::set<node_id> m_relayed_suppressed;
    detail::forward_dedup_table m_forward_dedup;
    endpoint_type m_endpoint;
    std::vector<remembered_demand> m_empty;
    std::vector<wire::topic_declaration> m_local_declarations;
    detail::egress_scheduler<channel_type, Policy> m_egress;
    std::unordered_map<std::uint64_t, detail::history_ring> m_retained;
    std::unordered_map<std::string, std::vector<remembered_demand>> m_remote_topics;
    // Self-route channel pointers recorded at attach_local, refcounted per fqn: the origination gate
    // excludes them from non-local demand so a same-node self-route never receives a forwarded envelope.
    std::unordered_map<const void *, std::size_t> m_self_route_channels;
    detail::companion_hooks m_companion_hooks;
    plexus::detail::move_only_function<void(const detail::drop_event &)> m_on_drop_cb;
    plexus::detail::move_only_function<void(std::uint64_t, const node_id &, std::uint8_t, std::span<const std::byte>, const channel_type *, const wire::shared_bytes *)> m_on_forward_refan_cb;
    plexus::detail::move_only_function<void(std::uint64_t, const node_id &, std::span<const std::byte>, const channel_type *)> m_on_originate_cb;
    plexus::detail::move_only_function<bool(const node_id &, const node_id &, std::uint8_t, std::span<const std::byte>)> m_on_forward_rpc_cb;
    plexus::detail::move_only_function<void(const graph::topic_edge &)> m_on_topic_edge_cb;
    plexus::detail::move_only_function<void(const node_id &, const wire::peer_report &)> m_on_peer_report_cb;
    plexus::detail::move_only_function<void(const wire::topic_declaration &)> m_on_declaration_cb;
    plexus::detail::move_only_function<bool(std::uint64_t)> m_capture_wants_payload_cb;
    plexus::detail::move_only_function<void(const qos_change_event &)> m_on_qos_change_cb;
    plexus::detail::move_only_function<void(const node_id &, std::uint64_t)> m_on_data_stamp_cb;
    plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_view &)> m_on_published_cb;
    plexus::detail::move_only_function<channel_type *(std::string_view, std::string_view, std::size_t)> m_companion_route_cb;
    plexus::detail::move_only_function<void(std::string_view, std::string_view, demand_transition, demand_role)> m_on_demand_transition_cb;
    plexus::detail::move_only_function<void(std::uint64_t, std::string_view, const message_info &, const message_view &)> m_on_delivered_cb;
};

}

#endif

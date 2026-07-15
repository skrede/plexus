#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_FORWARDER_H

#include "plexus/policy.h"
#include "plexus/node_id.h"
#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"
#include "plexus/publisher_gid.h"

#include "plexus/detail/compat.h"

#include "plexus/graph/topic_record.h"

#include "plexus/io/qos_rxo.h"
#include "plexus/io/locality.h"
#include "plexus/io/message_info.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/subscribe_qos_wire.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/io/subscription_endpoint.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/egress_scheduler.h"
#include "plexus/io/detail/forwarder_fanout.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

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

struct remembered_demand
{
    std::string fqn;
    subscriber_qos qos;
    std::optional<std::uint64_t> type_id; // nullopt = undeclared
};

constexpr std::size_t k_history_depth_cap = 1024; // DoS clamp: a publisher-declared depth is attacker-controlled (bounds N x max_payload).
constexpr std::size_t k_fetch_cap         = 1024;

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

    bool attach(const peer &p, std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt)
    {
        if(!m_endpoint.attach_gate(p.node_name, fqn))
            return false;
        auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, p.channel, p.node_name, qos, type_id);
        record_remote_topic(p.node_name, fqn, qos, type_id);
        send_subscribe(p.channel, fqn, hash, type_id, qos);
        emit_qos_change(qos_edge::accepted, hash, qos, rxo_verdict::compatible, type_id);
        emit_demand_transition(p.node_name, fqn, demand_transition::up, demand_role::subscriber);
        return true;
    }

    bool attach_for_fanout(const peer &p, std::string_view fqn, std::optional<std::uint64_t> subscriber_type_id = std::nullopt, const subscriber_qos &sub_qos = subscriber_qos{})
    {
        return detail::attach_for_fanout(*this, p, fqn, subscriber_type_id, sub_qos);
    }

    // Register a same-node self-route as a first-class subscriber: registry add only — no wire
    // subscribe, no remembered demand, no demand transition (the self-channel never dials and a
    // local subscription creates no wire demand). Idempotent via the registry's dedup by channel.
    void attach_local(std::string_view fqn, channel_type &channel, std::string_view self_name, const subscriber_qos &qos = subscriber_qos{},
                      std::optional<std::uint64_t> type_id = std::nullopt)
    {
        const auto hash = wire::fqn_topic_hash(fqn);
        m_endpoint.registry().add_subscriber(hash, fqn, channel, self_name, qos, type_id);
        replay_if_latched(peer{channel, std::string{self_name}}, hash);
    }

    void detach_local(std::string_view fqn, channel_type &channel)
    {
        m_endpoint.registry().remove_subscriber(wire::fqn_topic_hash(fqn), channel);
        m_egress.remove(channel);
    }

    void remember_demand(const std::string &node_name, std::string_view fqn, const subscriber_qos &qos = subscriber_qos{}, std::optional<std::uint64_t> type_id = std::nullopt)
    {
        record_remote_topic(node_name, fqn, qos, type_id);
    }

    void forget_remembered_demand(const std::string &node_name, std::string_view fqn)
    {
        forget_remote_topic(node_name, fqn);
    }

    void declare(std::string_view fqn, topic_qos qos, std::optional<std::uint64_t> producer_type_id = std::nullopt, bool emit_source_identity = false,
                 std::string_view producer_type_name = {}, std::uint64_t schema_hint = 0)
    {
        m_endpoint.registry().declare(wire::fqn_topic_hash(fqn), fqn, qos, producer_type_id, emit_source_identity, producer_type_name, schema_hint);
    }

    void latch(std::string_view fqn)
    {
        declare(fqn, topic_qos{.latch = true, .depth = 1});
    }

    void publish(std::string_view fqn, std::span<const std::byte> payload, std::uint64_t session_id = 0)
    {
        auto hash           = wire::fqn_topic_hash(fqn);
        const auto *topic   = m_endpoint.registry().entry_for(hash);
        const auto *subs    = topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
        const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
        if(subs == nullptr && !qos.latch)
            return;

        const std::optional<std::uint64_t> counter = topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
        message_info pub_info{};
        const wire_bytes<> framed = frame_publish(hash, payload, counter, session_id, pub_info);
        const message_view bare   = bare_of(framed, counter);
        emit_published(hash, fqn, bare);

        if(subs != nullptr)
            fan_published(*subs, hash, fqn, qos, framed, bare, pub_info);
        retain_if_latched(hash, qos, framed);
    }

    wire_bytes<> frame_publish(std::uint64_t hash, std::span<const std::byte> payload, std::optional<std::uint64_t> counter, std::uint64_t session_id, message_info &pub_info)
    {
        wire::unidirectional_header uhdr{.source = wire::endpoint_source_type::publisher, .sequence = m_endpoint.next_sequence(), .topic_hash = hash};
        wire::frame_header fhdr{.type         = wire::msg_type::unidirectional,
                                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                                .session_id   = session_id,
                                .timestamp_ns = wire::now_timestamp_ns(),
                                .payload_len  = 0};
        pub_info.publication_sequence = uhdr.sequence;
        pub_info.source_timestamp     = fhdr.timestamp_ns;
        return frame_owned(fhdr, uhdr, payload, counter);
    }

    wire_bytes<> frame_object(const object_carrier &carrier, std::uint64_t hash, std::optional<std::uint64_t> counter, std::uint64_t session_id, std::span<const std::byte> bytes)
    {
        wire::unidirectional_header uhdr{.source = wire::endpoint_source_type::publisher, .sequence = carrier.sequence, .topic_hash = hash};
        wire::frame_header fhdr{.type         = wire::msg_type::unidirectional,
                                .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                                .session_id   = session_id,
                                .timestamp_ns = carrier.source_timestamp,
                                .payload_len  = 0};
        return frame_owned(fhdr, uhdr, bytes, counter);
    }

    void fan_published(const std::vector<typename subscriber_registry<channel_type>::subscriber> &subs, std::uint64_t hash, std::string_view fqn, const topic_qos &qos,
                       const wire_bytes<> &framed, const message_view &bare, const message_info &pub_info)
    {
        const std::size_t band = detail::band_of(qos.priority);
        for(const auto &sub : subs)
        {
            if(!any_set(qos.reach, sub.tier))
                continue;

            channel_type *route =
                    &sub.channel
                             .get(); // Gate on the FRAMED size, not the bare payload: the ring slot carries framed bytes, so a bare-size gate would route a frame-overflowing message to SHM only and drop it.
            if(m_companion_route_cb)
                if(channel_type *companion = m_companion_route_cb(sub.node_name, fqn, framed.size()))
                    route = companion;

            const detail::drop_cause cause = m_egress.enqueue(*route, band, qos.congestion, framed);
            if(cause != detail::drop_cause::none)
                shed(hash, band, cause, sub.tier);
            emit_delivered(hash, fqn, pub_info, bare);
        }
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
            framed  = frame_object(carrier, hash, counter, session_id, std::forward<EncodeFn>(encode)());
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
            retain_if_latched(hash, qos, framed);
        }

        release(carrier);
    }

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
        emit_qos_change(qos_edge::unsubscribed, hash, subscriber_qos{}, rxo_verdict::compatible, std::nullopt);
        emit_demand_transition(p.node_name, fqn, demand_transition::down, demand_role::subscriber);
        return true;
    }

    void detach_all(const peer &p)
    {
        m_endpoint.remove_peer(p);
        m_egress.remove(p.channel);
    }

    const std::vector<remembered_demand> &remembered_topics(const std::string &node_name) const
    {
        auto it = m_remote_topics.find(node_name);
        return it == m_remote_topics.end() ? m_empty : it->second;
    }

    template<typename OnMessage>
    void deliver(const peer &, std::span<const std::byte> inner, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
        auto decoded = wire::decode_unidirectional(inner, has_source_identity);
        if(!decoded)
            return drop("plexus: forwarder unidirectional_decode_failed");

        auto fqn = m_endpoint.registry().fqn_for(decoded->header.topic_hash);
        if(fqn.empty())
            return drop("plexus: forwarder topic_unknown_for_data");

        stamp_received(source_node_id, decoded->header.topic_hash);
        on_message(fqn, decoded->data);
    }

    template<typename OnMessage>
    void deliver(const peer &, std::span<const std::byte> inner, message_info info, const node_id &source_node_id, bool has_source_identity, OnMessage &&on_message)
    {
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

    void fetch_latched(const peer &p, std::uint64_t topic_hash, std::uint32_t max_samples)
    {
        auto it = m_retained.find(topic_hash);
        if(it == m_retained.end() || it->second.empty())
            return;

        const auto limit = std::min<std::size_t>({static_cast<std::size_t>(max_samples), it->second.count(), k_fetch_cap});
        replay_window(p, it->second, limit);
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

    void send_control(channel_type &channel, wire::msg_type type, std::span<const std::byte> inner)
    {
        m_endpoint.send_control(channel, type, inner);
    }

    wire_bytes<> frame_owned(const wire::frame_header &fhdr, const wire::unidirectional_header &uhdr, std::span<const std::byte> payload, std::optional<std::uint64_t> counter)
    {
        auto buf = std::make_shared<std::vector<std::byte>>();
        wire::encode_unidirectional_frame_into(*buf, fhdr, uhdr, payload, counter);
        std::span<const std::byte> view{*buf};
        return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
    }

    static message_view bare_of(const wire_bytes<> &framed, std::optional<std::uint64_t> counter)
    {
        const std::size_t prefix              = wire::header_size + wire::unidirectional_header_size + (counter ? wire::varint_size(*counter) : 0);
        const std::span<const std::byte> view = static_cast<std::span<const std::byte>>(framed);
        return message_view{view.subspan(prefix), framed.owner()};
    }

    void retain_if_latched(std::uint64_t hash, const topic_qos &qos, const wire_bytes<> &framed)
    {
        if(!qos.latch)
            return;

        auto &ring = m_retained[hash];
        ring.resize_to(std::clamp<std::size_t>(qos.depth, 1, k_history_depth_cap));
        ring.push(static_cast<std::span<const std::byte>>(framed));
    }

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
                    auto count = it->second.count();
                    auto limit = sub.replay_depth ? std::min<std::size_t>(count, sub.replay_depth) : count;
                    replay_window(p, it->second, limit);
                }
        }
    }

    void replay_window(const peer &p, const detail::history_ring &ring, std::size_t limit)
    {
        const std::size_t count = ring.count();
        for(std::size_t i = count - limit; i < count; ++i)
            p.channel.send(ring.oldest_to_newest(i));
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

    static wire::subscribe_status status_of(io::rxo_verdict v)
    {
        return v == io::rxo_verdict::source_identity_incompatible ? wire::subscribe_status::source_identity_incompatible : wire::subscribe_status::incompatible_qos;
    }

    void send_subscribe(channel_type &channel, std::string_view fqn, std::uint64_t hash, std::optional<std::uint64_t> type_id = std::nullopt,
                        const subscriber_qos &sub_qos = subscriber_qos{})
    {
        wire::subscribe_request req{.fqn        = std::string{fqn},
                                    .type_name  = {},
                                    .topic_hash = hash,
                                    .type_hash  = type_id.value_or(0), // 0 = undeclared
                                    .source     = wire::endpoint_source_type::publisher};

        if(!(sub_qos == subscriber_qos{}))
        {
            req.has_qos = true;
            req.qos     = to_wire_region(sub_qos);
        }
        m_endpoint.send_subscribe(channel, req);
    }

    void record_remote_topic(const std::string &node_name, std::string_view fqn, const subscriber_qos &qos, std::optional<std::uint64_t> type_id = std::nullopt)
    {
        auto &topics = m_remote_topics[node_name];
        for(const auto &existing : topics)
            if(existing.fqn == fqn)
                return; // idempotent: keep the first stored qos + type_id
        topics.emplace_back(remembered_demand{std::string{fqn}, qos, type_id});
    }

    void forget_remote_topic(const std::string &node_name, std::string_view fqn)
    {
        auto it = m_remote_topics.find(node_name);
        if(it == m_remote_topics.end())
            return;
        std::erase_if(it->second, [&](const remembered_demand &d) { return d.fqn == fqn; });
        if(it->second.empty())
            m_remote_topics.erase(it);
    }

    void stamp_received(const node_id &source_node_id, std::uint64_t topic_hash)
    {
        if(m_on_data_stamp_cb)
            m_on_data_stamp_cb(source_node_id, topic_hash);
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

    void drop(std::string_view message)
    {
        m_logger.warn(message);
    }

    log::logger &m_logger;
    std::size_t m_global_default;
    endpoint_type m_endpoint;
    std::vector<remembered_demand> m_empty;
    detail::egress_scheduler<channel_type, Policy> m_egress;
    std::unordered_map<std::uint64_t, detail::history_ring> m_retained;
    std::unordered_map<std::string, std::vector<remembered_demand>> m_remote_topics;
    plexus::detail::move_only_function<void(const detail::drop_event &)> m_on_drop_cb;
    plexus::detail::move_only_function<void(const graph::topic_edge &)> m_on_topic_edge_cb;
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

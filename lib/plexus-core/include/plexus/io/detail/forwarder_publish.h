#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_PUBLISH_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_PUBLISH_H

#include "plexus/io/locality.h"
#include "plexus/io/message_info.h"
#include "plexus/io/history_limits.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/observation_events.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/history_ring.h"
#include "plexus/io/detail/priority_band_queue.h"

#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::io::detail {

inline wire_bytes<> frame_owned(const wire::frame_header &fhdr, const wire::unidirectional_header &uhdr, std::span<const std::byte> payload, std::optional<std::uint64_t> counter)
{
    auto buf = std::make_shared<std::vector<std::byte>>();
    wire::encode_unidirectional_frame_into(*buf, fhdr, uhdr, payload, counter);
    std::span<const std::byte> view{*buf};
    return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
}

inline wire_bytes<> frame_object(const object_carrier &carrier, std::uint64_t hash, std::optional<std::uint64_t> counter, std::uint64_t session_id, std::span<const std::byte> bytes)
{
    wire::unidirectional_header uhdr{.source = wire::endpoint_source_type::publisher, .sequence = carrier.sequence, .topic_hash = hash};
    wire::frame_header fhdr{.type         = wire::msg_type::unidirectional,
                            .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                            .session_id   = session_id,
                            .timestamp_ns = carrier.source_timestamp,
                            .payload_len  = 0};
    return frame_owned(fhdr, uhdr, bytes, counter);
}

template<typename Forwarder>
wire_bytes<> frame_publish(Forwarder &f, std::uint64_t hash, std::span<const std::byte> payload, std::optional<std::uint64_t> counter, std::uint64_t session_id, message_info &pub_info)
{
    wire::unidirectional_header uhdr{.source = wire::endpoint_source_type::publisher, .sequence = f.m_endpoint.next_sequence(), .topic_hash = hash};
    wire::frame_header fhdr{.type         = wire::msg_type::unidirectional,
                            .flags        = counter ? wire::k_flag_source_identity : std::uint8_t{0},
                            .session_id   = session_id,
                            .timestamp_ns = wire::now_timestamp_ns(),
                            .payload_len  = 0};
    pub_info.publication_sequence = uhdr.sequence;
    pub_info.source_timestamp     = fhdr.timestamp_ns;
    return frame_owned(fhdr, uhdr, payload, counter);
}

template<typename Forwarder>
void retain_if_latched(Forwarder &f, std::uint64_t hash, const topic_qos &qos, const wire_bytes<> &framed)
{
    if(!qos.latch)
        return;

    auto &ring = f.m_retained[hash];
    ring.resize_to(std::clamp<std::size_t>(qos.depth, 1, k_history_depth_cap));
    ring.push(static_cast<std::span<const std::byte>>(framed));
}

template<typename Peer>
void replay_window(const Peer &p, const detail::history_ring &ring, std::size_t limit)
{
    const std::size_t count = ring.count();
    for(std::size_t i = count - limit; i < count; ++i)
        p.channel.send(ring.oldest_to_newest(i));
}

template<typename Forwarder, typename Peer>
void replay_if_latched(Forwarder &f, const Peer &p, std::uint64_t hash)
{
    if(!f.m_endpoint.registry().qos_for(hash).latch)
        return;

    auto it = f.m_retained.find(hash);
    if(it == f.m_retained.end() || it->second.empty())
        return;

    const subscriber_qos sub = f.m_endpoint.registry().qos_for_subscriber(hash, p.channel);
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

template<typename Forwarder, typename Subscribers>
void fan_published(Forwarder &f, const Subscribers &subs, std::uint64_t hash, std::string_view fqn, const topic_qos &qos, const wire_bytes<> &framed, const message_view &bare,
                   const message_info &pub_info)
{
    const std::size_t band = detail::band_of(qos.priority);
    for(const auto &sub : subs)
    {
        if(!any_set(qos.reach, sub.tier))
            continue;

        // Gate on the FRAMED size, not the bare payload: the ring slot carries framed bytes, so a bare-size gate would route a frame-overflowing message to SHM only and drop it.
        typename Forwarder::channel_type *route = &sub.channel.get();
        if(f.m_companion_route_cb)
            if(typename Forwarder::channel_type *companion = f.m_companion_route_cb(sub.node_name, fqn, framed.size()))
                route = companion;

        const detail::drop_cause cause = f.m_egress.enqueue(*route, band, qos.congestion, framed);
        if(cause != detail::drop_cause::none)
            f.shed(hash, band, cause, sub.tier);
        f.emit_delivered(hash, fqn, pub_info, bare);
    }
}

template<typename Forwarder>
void forwarder_publish(Forwarder &f, std::string_view fqn, std::span<const std::byte> payload, std::uint64_t session_id)
{
    auto hash           = wire::fqn_topic_hash(fqn);
    const auto *topic   = f.m_endpoint.registry().entry_for(hash);
    const auto *subs    = topic != nullptr && !topic->subscribers.empty() ? &topic->subscribers : nullptr;
    const topic_qos qos = topic != nullptr ? topic->qos : topic_qos{};
    if(subs == nullptr && !qos.latch)
        return;

    const std::optional<std::uint64_t> counter = topic->emit_source_identity ? topic->endpoint_counter : std::nullopt;
    message_info pub_info{};
    const wire_bytes<> framed = frame_publish(f, hash, payload, counter, session_id, pub_info);
    const message_view bare   = Forwarder::bare_of(framed, counter);
    f.emit_published(hash, fqn, bare);

    if(subs != nullptr)
        fan_published(f, *subs, hash, fqn, qos, framed, bare, pub_info);
    retain_if_latched(f, hash, qos, framed);
}

}

#endif

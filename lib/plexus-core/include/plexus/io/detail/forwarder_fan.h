#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FAN_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FAN_H

#include "plexus/io/locality.h"
#include "plexus/io/congestion.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/observation_events.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/detail/forwarder_companion.h"
#include "plexus/io/detail/subscriber_dispatch.h"

#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::detail {

// A late companion takes over only once nothing is queued toward the wire channel, so a ring frame never overtakes a wire
// frame still waiting in the bands; the switch then holds until the companion route goes away.
template<typename Channel, typename Route, typename Egress>
Channel *subscriber_route(Route &route_cb, const Egress &egress, const registry_subscriber<Channel> &sub, std::string_view fqn, std::size_t bytes)
{
    Channel *companion = route_cb(sub.node_name, fqn, bytes);
    if(companion == nullptr)
        sub.on_companion = false;
    else if(!sub.on_companion)
        sub.on_companion = egress.idle(sub.channel.get());
    return sub.on_companion ? companion : &sub.channel.get();
}

// Every subscriber routed to a topic's lane reads the one ring behind it, so a publish writes the lane at most once and
// each of those subscribers sheds that one write's drop.
template<typename Channel>
struct lane_write
{
    Channel *lane;
    bool written;
    drop_cause cause;
};

// Gate on the FRAMED size, not the bare payload: the ring slot carries framed bytes, so a bare-size gate would route a
// frame-overflowing message to shared memory only and drop it.
template<typename Route, typename Egress, typename Channel>
drop_cause enqueue_routed(Route &route_cb, Egress &egress, lane_write<Channel> &once, const registry_subscriber<Channel> &sub, std::string_view fqn, std::size_t band,
                          io::congestion congestion, const wire_bytes<> &framed)
{
    Channel *route = route_cb ? subscriber_route(route_cb, egress, sub, fqn, framed.size()) : &sub.channel.get();
    if(route != once.lane)
        return egress.enqueue(*route, band, congestion, framed);
    if(!once.written)
    {
        once.cause   = egress.enqueue(*route, band, congestion, framed);
        once.written = true;
    }
    return once.cause;
}

// A process-tier subscriber of the carrier's own type takes the object by address; every other one takes framed bytes.
template<typename Channel>
bool sent_as_object(const registry_subscriber<Channel> &sub, const object_carrier &carrier)
{
    if constexpr(requires(Channel &c) { c.send_object(carrier); })
    {
        if(sub.tier != locality::process || !sub.type_id || *sub.type_id != carrier.type_tag)
            return false;
        sub.channel.get().send_object(carrier);
        return true;
    }
    else
        return false;
}

template<typename Forwarder, typename Subscribers>
void fan_published(Forwarder &f, const Subscribers &subs, std::uint64_t hash, std::string_view fqn, const topic_qos &qos, const wire_bytes<> &framed, const message_view &bare,
                   const message_info &pub_info)
{
    const std::size_t band = detail::band_of(qos.priority);
    lane_write<typename Forwarder::channel_type> once{lane_channel(f.m_companion_hooks, fqn), false, drop_cause::none};
    for(const auto &sub : subs)
    {
        if(!any_set(qos.reach, sub.tier))
            continue;
        const drop_cause cause = enqueue_routed(f.m_companion_route_cb, f.m_egress, once, sub, fqn, band, qos.congestion, framed);
        if(cause != drop_cause::none)
            f.shed(hash, band, cause, sub.tier);
        f.emit_delivered(hash, fqn, pub_info, bare);
    }
}

// framed is encoded on the first subscriber that needs bytes; encode_once fills it.
template<typename Forwarder, typename Subscribers, typename EncodeOnce>
void fan_object(Forwarder &f, const Subscribers &subs, std::uint64_t hash, std::string_view fqn, const topic_qos &qos, const object_carrier &carrier, const wire_bytes<> &framed,
                const message_view &bare, const message_info &obj_info, EncodeOnce &encode_once)
{
    const std::size_t band = detail::band_of(qos.priority);
    lane_write<typename Forwarder::channel_type> once{lane_channel(f.m_companion_hooks, fqn), false, drop_cause::none};
    for(const auto &sub : subs)
    {
        if(!any_set(qos.reach, sub.tier))
            continue;
        if(!sent_as_object(sub, carrier))
        {
            encode_once();
            const drop_cause cause = enqueue_routed(f.m_companion_route_cb, f.m_egress, once, sub, fqn, band, qos.congestion, framed);
            if(cause != drop_cause::none)
                f.shed(hash, band, cause, sub.tier);
        }
        f.emit_delivered(hash, fqn, obj_info, bare);
    }
}

}

#endif

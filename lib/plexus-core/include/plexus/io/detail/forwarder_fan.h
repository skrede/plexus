#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FAN_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_FORWARDER_FAN_H

#include "plexus/io/locality.h"
#include "plexus/io/congestion.h"
#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/upgrade_channel.h"
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

// A subscriber switches to its lane on the first message that fits once nothing is queued toward its wire channel, so no
// ring frame overtakes a wire frame still in the bands, and the marker it is sent on the wire first names that message.
// The switch is one-way: moving the subscriber back for an over-cap message would put its next messages on the wire while
// other readers' copies of them land in the ring it reads, so it rides the lane until the route offers no lane.
template<typename Forwarder, typename Channel>
Channel *subscriber_route(Forwarder &f, const registry_subscriber<Channel> &sub, std::string_view fqn, std::size_t bytes, std::uint64_t sequence)
{
    const lane_route<Channel> route = f.m_companion_route_cb(sub.node_name, fqn, bytes);
    if(route.lane == nullptr)
        sub.on_companion = false;
    else if(!sub.on_companion && route.fits && f.m_egress.idle(sub.channel.get()) && place_reader(f.m_companion_hooks, fqn, route.reader))
    {
        sub.on_companion = true;
        send_switch(f.m_endpoint, sub.channel.get(), fqn, sequence);
    }
    return sub.on_companion && route.fits ? route.lane : &sub.channel.get();
}

// Every subscriber routed to a topic's lane reads the one ring behind it, so a publish writes the lane at most once and
// each of those subscribers sheds that one write's drop.
template<typename Channel>
struct lane_write
{
    Channel *lane;
    std::uint64_t sequence;
    bool written;
    drop_cause cause;
};

// Gate on the FRAMED size, not the bare payload: the ring slot carries framed bytes, so a bare-size gate would route a
// frame-overflowing message to shared memory only and drop it.
template<typename Forwarder, typename Channel>
drop_cause enqueue_routed(Forwarder &f, lane_write<Channel> &once, const registry_subscriber<Channel> &sub, std::string_view fqn, std::size_t band, io::congestion congestion,
                          const wire_bytes<> &framed)
{
    Channel *route = f.m_companion_route_cb ? subscriber_route(f, sub, fqn, framed.size(), once.sequence) : &sub.channel.get();
    if(route != once.lane)
        return f.m_egress.enqueue(*route, band, congestion, framed);
    if(!once.written)
    {
        once.cause   = f.m_egress.enqueue(*route, band, congestion, framed);
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
    lane_write<typename Forwarder::channel_type> once{lane_channel(f.m_companion_hooks, fqn), pub_info.publication_sequence, false, drop_cause::none};
    for(const auto &sub : subs)
    {
        if(!any_set(qos.reach, sub.tier))
            continue;
        const drop_cause cause = enqueue_routed(f, once, sub, fqn, band, qos.congestion, framed);
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
    lane_write<typename Forwarder::channel_type> once{lane_channel(f.m_companion_hooks, fqn), carrier.sequence, false, drop_cause::none};
    for(const auto &sub : subs)
    {
        if(!any_set(qos.reach, sub.tier))
            continue;
        if(!sent_as_object(sub, carrier))
        {
            encode_once();
            const drop_cause cause = enqueue_routed(f, once, sub, fqn, band, qos.congestion, framed);
            if(cause != drop_cause::none)
                f.shed(hash, band, cause, sub.tier);
        }
        f.emit_delivered(hash, fqn, obj_info, bare);
    }
}

}

#endif

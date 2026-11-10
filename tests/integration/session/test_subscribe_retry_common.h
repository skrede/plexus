#ifndef HPP_GUARD_TESTS_INTEGRATION_SUBSCRIBE_RETRY_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SUBSCRIBE_RETRY_COMMON_H

#include "test_subscribe_retry_relay.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/subscribe_retry_config.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/graph/topic_record.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/topic_qos.h"

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace subscribe_retry_fixture {

using namespace routing_inproc_fixture;
namespace wire = plexus::wire;
using plexus::io::subscribe_retry_config;
using plexus::io::detail::drop_event;
using retry_channel    = inproc_channel<manual_clock>;
using retry_session    = plexus::io::peer_session<manual_policy>;
using retry_messages   = plexus::io::message_forwarder<manual_policy>;
using retry_procedures = plexus::io::procedure_forwarder<manual_policy>;

constexpr std::string_view k_topic    = "retry/topic";
constexpr std::string_view k_other    = "retry/other";
constexpr std::string_view k_latched  = "retry/latched";
constexpr std::string_view k_declared = "retry/declared";

// Two real sessions on the manual clock whose channels meet only at a relay, so a test decides per frame and per
// direction whether it passes, waits or is lost. The requester dials and publishes; the responder accepts.
struct retry_link
{
    retry_link()
            : bus()
            , ex(bus)
            , near(ex)
            , far(ex)
            , sink()
            , req_messages(sink)
            , resp_messages(sink)
            , req_procedures(ex, k_long_timeout, sink)
            , resp_procedures(ex, k_long_timeout, sink)
            , req_ctx()
            , resp_ctx()
            , requester()
            , responder()
            , to_requester()
            , to_responder()
            , req_drops()
            , resp_drops()
            , req_declared()
            , resp_received()
            , resp_refusals()
            , req_stamps(0)
            , resp_ready(0)
    {
        manual_clock::reset();
        req_messages.declare(k_topic, plexus::topic_qos{});
        req_messages.declare(k_other, plexus::topic_qos{});
        req_messages.declare(k_latched, plexus::topic_qos{.latch = true, .depth = 1});
        req_messages.publish(k_latched, as_bytes("latched"));
        req_messages.on_topic_edge([this](const plexus::graph::topic_edge &e) { req_declared.emplace_back(e.topic); });
        build(req_ctx, near, requester, req_messages, req_procedures, false);
        build(resp_ctx, far, responder, resp_messages, resp_procedures, true);
        requester->on_frame_dropped([this](const drop_event &e) { req_drops.push_back(e); });
        requester->on_stamp_seen([this](const plexus::node_id &) { ++req_stamps; });
        responder->on_frame_dropped([this](const drop_event &e) { resp_drops.push_back(e); });
        responder->on_subscribe_refused([this](std::uint64_t hash, wire::subscribe_status status) { resp_refusals.emplace_back(hash, status); });
        responder->on_message([this](std::string_view, std::span<const std::byte> d) { resp_received.emplace_back(to_string(d)); });
        responder->on_lifecycle([this](const plexus::io::lifecycle_event &e) { resp_ready += e.edge == plexus::io::lifecycle_edge::ready ? 1u : 0u; });
        near.on_data([this](std::span<const std::byte> f) { carry(to_responder, far, f); });
        far.on_data([this](std::span<const std::byte> f) { carry(to_requester, near, f); });
    }

    void enable_retry(retry_session &s, const subscribe_retry_config &cfg = {})
    {
        s.set_subscribe_retry(cfg);
    }

    void demand(std::string_view fqn, const plexus::io::subscriber_qos &qos = {})
    {
        resp_messages.remember_demand(resp_ctx.node_name, fqn, qos);
    }

    void start()
    {
        requester->start();
        responder->start();
        ex.drain();
    }

    void advance(std::chrono::milliseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }

    void publish(std::string_view fqn, const std::string &payload)
    {
        req_messages.publish(fqn, as_bytes(payload));
        ex.drain();
    }

    void release(relay_leg &leg)
    {
        retry_channel &out = &leg == &to_requester ? near : far;
        for(const auto &f : std::exchange(leg.held, {}))
            out.send(f);
        ex.drain();
    }

    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    retry_channel near;
    retry_channel far;
    recording_logger sink;
    retry_messages req_messages;
    retry_messages resp_messages;
    retry_procedures req_procedures;
    retry_procedures resp_procedures;
    plexus::io::peer_context<manual_policy> req_ctx;
    plexus::io::peer_context<manual_policy> resp_ctx;
    std::optional<retry_session> requester;
    std::optional<retry_session> responder;
    relay_leg to_requester;
    relay_leg to_responder;
    std::vector<drop_event> req_drops;
    std::vector<drop_event> resp_drops;
    std::vector<std::string> req_declared;
    std::vector<std::string> resp_received;
    std::vector<std::pair<std::uint64_t, wire::subscribe_status>> resp_refusals;
    std::uint32_t req_stamps;
    std::uint32_t resp_ready;

private:
    void build(plexus::io::peer_context<manual_policy> &ctx, retry_channel &relay_end, std::optional<retry_session> &s, retry_messages &m, retry_procedures &p, bool accepted)
    {
        ctx.channel = std::make_unique<retry_channel>(ex);
        ctx.channel->connect_to(relay_end.local_endpoint());
        relay_end.connect_to(ctx.channel->local_endpoint());
        ctx.node_name = accepted ? "requester-node" : "responder-node";
        ctx.peer_id   = make_cfg(accepted ? 0x02 : 0x01).self_id;
        s.emplace(ctx, ex, make_cfg(accepted ? 0x01 : 0x02), k_long_timeout, m, p, accepted, sink);
    }

    void carry(relay_leg &leg, retry_channel &out, std::span<const std::byte> frame)
    {
        const auto type   = wire::decode_header(frame)->type;
        const auto nth    = ++leg.seen[static_cast<std::uint8_t>(type)];
        const auto action = leg.rule ? leg.rule(type, nth) : relay_action::pass;
        if(action == relay_action::pass)
            out.send(frame);
        else if(action == relay_action::hold)
            leg.held.emplace_back(frame.begin(), frame.end());
    }
};

}

#endif

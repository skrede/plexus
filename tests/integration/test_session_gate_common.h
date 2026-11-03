#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_GATE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_GATE_COMMON_H

#include "test_session_gate_frames.h"
#include "test_peer_session_inproc_common.h"

#include "plexus/io/peer_context.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"
#include "plexus/topic_qos.h"

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace session_gate_fixture {

constexpr std::array k_all_types{
        wire::msg_type::unidirectional,     wire::msg_type::bidirectional,       wire::msg_type::handshake_req,    wire::msg_type::handshake_resp,  wire::msg_type::subscribe,
        wire::msg_type::unsubscribe,        wire::msg_type::fetch_latched,       wire::msg_type::fetch_metadata,   wire::msg_type::rpc_request,     wire::msg_type::rpc_response,
        wire::msg_type::subscribe_response, wire::msg_type::heartbeat,           wire::msg_type::declare,          wire::msg_type::peer_report,     wire::msg_type::forwarded,
        wire::msg_type::companion_ready,    wire::msg_type::companion_withdrawn, wire::msg_type::companion_switch, wire::msg_type::companion_offer,
};
static_assert(k_all_types.size() == 19);

constexpr bool is_handshake(wire::msg_type type)
{
    return type == wire::msg_type::handshake_req || type == wire::msg_type::handshake_resp;
}

struct effects
{
    std::uint32_t stamps{0};
    std::uint32_t messages{0};
    std::uint32_t peer_frames{0};
    std::uint32_t handler_runs{0};
};

// One real session over the inproc rendezvous whose peer end is a bare channel: the test speaks for the peer, so no
// frame reaches the session unless the test sends it.
struct lone_session
{
    lone_session(bool accepted, const handshake_fsm_config &self)
            : fx()
            , bus()
            , ex(bus)
            , transport(ex, bus)
            , sink()
            , messages(sink)
            , procedures(ex, k_long_timeout, sink)
            , ctx()
            , subject()
            , last_peer_frame()
            , edges()
            , peer_channel()
    {
        prime();
        transport.on_accepted([this, accepted, self](std::unique_ptr<inproc_channel<>> ch) { accepted ? build(std::move(ch), self, true) : hold(std::move(ch)); });
        transport.on_dialed([this, accepted, self](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
                            { accepted ? hold(std::move(ch)) : build(std::move(ch), self, false); });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
        settle();
    }

    void hear(std::span<const std::byte> frame)
    {
        peer_channel->send(frame);
        settle();
    }

    void inject(std::span<const std::byte> frame)
    {
        subject->inject_receive(frame);
        ex.drain();
    }

    void settle()
    {
        ex.drain();
        fx = effects{};
    }

    bool fanned_out() const
    {
        return messages.has_remote_demand(wire::fqn_topic_hash(k_topic), nullptr);
    }

    effects fx;
    inproc_bus<> bus;
    inproc_executor<> ex;
    inproc_transport<> transport;
    plexus::log::null_logger sink;
    msg_forwarder messages;
    rpc_forwarder procedures;
    plexus::io::peer_context<inproc_policy> ctx;
    std::optional<session> subject;
    std::vector<std::byte> last_peer_frame;
    std::vector<plexus::io::lifecycle_edge> edges;
    std::unique_ptr<inproc_channel<>> peer_channel;

private:
    void prime()
    {
        messages.declare(k_topic, plexus::topic_qos{});
        messages.latch(k_latched);
        messages.publish(k_latched, as_bytes("latched"));
        procedures.serve(k_procedure,
                         [this](std::span<const std::byte>, rpc_forwarder::reply_fn &reply)
                         {
                             ++fx.handler_runs;
                             reply(rpc_status::success, {});
                         });
    }

    void hold(std::unique_ptr<inproc_channel<>> ch)
    {
        peer_channel = std::move(ch);
        peer_channel->on_data(
                [this](std::span<const std::byte> f)
                {
                    ++fx.peer_frames;
                    last_peer_frame.assign(f.begin(), f.end());
                });
    }

    void build(std::unique_ptr<inproc_channel<>> ch, const handshake_fsm_config &self, bool accepted)
    {
        ctx.channel   = std::move(ch);
        ctx.node_name = "peer-node";
        ctx.peer_id   = make_cfg(accepted ? 0x02 : 0x01).self_id;
        subject.emplace(ctx, ex, self, k_long_timeout, messages, procedures, accepted, sink);
        subject->on_message([this](std::string_view, std::span<const std::byte>) { ++fx.messages; });
        subject->on_stamp_seen([this](const plexus::node_id &) { ++fx.stamps; });
        subject->on_lifecycle([this](const plexus::io::lifecycle_event &e) { edges.push_back(e.edge); });
        subject->start();
    }
};

struct pre_handshake_acceptor : lone_session
{
    pre_handshake_acceptor()
            : lone_session(true, make_cfg(0x01))
    {
    }
};

struct pre_response_dialer : lone_session
{
    pre_response_dialer()
            : lone_session(false, make_cfg(0x02))
    {
    }
};

inline std::vector<std::byte> handshake_request_frame()
{
    return pre_response_dialer{}.last_peer_frame;
}

struct established_acceptor : lone_session
{
    established_acceptor()
            : lone_session(true, make_cfg(0x01))
    {
        hear(handshake_request_frame());
    }
};

}

#endif

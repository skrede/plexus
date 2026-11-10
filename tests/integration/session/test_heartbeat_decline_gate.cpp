// The heartbeat decline-consumption authentication gate. The relay's cooperative-decline honor state is
// mutated only by an authenticated peer: a decline-flagged heartbeat that arrives BEFORE the handshake
// settles who is speaking must not be consumed, or it would fold a decline under the default node_id{}
// the pre-handshake session still carries. Both proofs drive a REAL peer_session's inject_receive:
//
//   1. A pre-handshake decline heartbeat fires no decline callback (the is_complete gate drops it).
//   2. The positive control — the same frame on a completed session IS consumed, attributed to the
//      proven peer identity, never node_id{}.

#include "test_peer_session_inproc_common.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/inproc/inproc_channel.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

using namespace peer_session_inproc_fixture;
namespace wire = plexus::wire;
using plexus::node_id;

namespace {

std::vector<std::byte> heartbeat_decline_frame()
{
    wire::heartbeat hb;
    hb.reserved |= wire::k_heartbeat_relay_decline_flag;
    const auto payload = wire::encode_heartbeat(hb);
    wire::frame_header fhdr{.type = wire::msg_type::heartbeat, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = payload.size()};
    return wire::encode_frame(fhdr, payload);
}

// A single accepted responder that never receives a handshake, so it stays incomplete: the peer's dialer
// channel is held open (no session drives it) so the responder's channel stays live and its consumers are
// installed, but last_seen_peer_id() is still the default node_id{} — the exact state a decline heartbeat
// must not mutate.
struct lone_responder
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    plexus::log::null_logger sink;
    msg_forwarder resp_messages{sink};
    rpc_forwarder resp_procedures{ex, k_long_timeout, sink};

    plexus::io::peer_context<inproc_policy> resp_ctx;
    std::unique_ptr<plexus::inproc::inproc_channel<>> dialer_channel;
    std::optional<session> responder;

    lone_responder()
    {
        transport.on_accepted(
                [this](std::unique_ptr<plexus::inproc::inproc_channel<>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    resp_ctx.peer_id   = make_cfg(0x02).self_id;
                    responder.emplace(resp_ctx, ex, make_cfg(0x01), k_long_timeout, resp_messages, resp_procedures, true, sink);
                    responder->start();
                });
        transport.on_dialed([this](std::unique_ptr<plexus::inproc::inproc_channel<>> ch, const plexus::io::endpoint &) { dialer_channel = std::move(ch); });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
        ex.drain();
    }
};

}

TEST_CASE("heartbeat_decline_gate: a pre-handshake decline heartbeat does not mutate the relay's decline state", "[integration][heartbeat][decline]")
{
    lone_responder link;
    REQUIRE(link.responder.has_value());
    REQUIRE_FALSE(link.responder->is_complete());

    std::vector<std::pair<node_id, bool>> declines;
    link.responder->on_decline_seen([&](const node_id &id, bool declining) { declines.emplace_back(id, declining); });

    link.responder->inject_receive(heartbeat_decline_frame());
    link.ex.drain();

    REQUIRE(declines.empty());
    REQUIRE_FALSE(link.responder->is_complete());
}

TEST_CASE("heartbeat_decline_gate: a decline heartbeat on a completed session is honored under the proven identity", "[integration][heartbeat][decline]")
{
    session_link link;
    link.drive();
    REQUIRE(link.responder->is_complete());

    std::vector<std::pair<node_id, bool>> declines;
    link.responder->on_decline_seen([&](const node_id &id, bool declining) { declines.emplace_back(id, declining); });

    link.responder->inject_receive(heartbeat_decline_frame());
    link.drive();

    REQUIRE(declines.size() == 1);
    REQUIRE(declines.front().second);
    REQUIRE(declines.front().first == link.responder->peer_identity());
    REQUIRE(declines.front().first != node_id{});
}

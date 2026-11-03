#include "test_session_gate_common.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_context.h"

#include "plexus/io/detail/drop_event.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

using namespace session_gate_fixture;
using plexus::io::detail::drop_cause;
using engine_clock = routing_inproc_fixture::manual_clock;

namespace {

// A dialer whose peer answers only when the test says so: the peer session is built late on the bare channel and handed
// the request the dialer sent at start, so the test controls what the dialer hears before its response.
struct answered_dialer
{
    answered_dialer()
            : dialer()
            , messages(dialer.sink)
            , procedures(dialer.ex, k_long_timeout, dialer.sink)
            , request(dialer.last_peer_frame)
            , received()
            , ctx()
            , peer()
    {
        dialer.subject->on_message([this](std::string_view, std::span<const std::byte> d) { received.emplace_back(to_string(d)); });
    }

    void answer()
    {
        ctx.channel   = std::move(dialer.peer_channel);
        ctx.node_name = "dialer-node";
        ctx.peer_id   = make_cfg(0x02).self_id;
        peer.emplace(ctx, dialer.ex, make_cfg(0x01), k_long_timeout, messages, procedures, true, dialer.sink);
        peer->start();
        peer->inject_receive(request);
        dialer.ex.drain();
        dialer.subject->subscribe(k_topic);
        dialer.ex.drain();
    }

    void publish(const std::string &payload)
    {
        messages.publish(k_topic, as_bytes(payload), peer->session_id());
        dialer.ex.drain();
    }

    pre_response_dialer dialer;
    msg_forwarder messages;
    rpc_forwarder procedures;
    std::vector<std::byte> request;
    std::vector<std::string> received;
    plexus::io::peer_context<inproc_policy> ctx;
    std::optional<session> peer;
};

struct drop_recorder final : plexus::io::observer
{
    void on_drop(const plexus::io::detail::drop_event &ev) override
    {
        seen.push_back(ev);
    }

    std::vector<plexus::io::detail::drop_event> seen;
};

}

TEST_CASE("session gate latch: a stray frame before completion does not latch a session id", "[integration][session_gate]")
{
    answered_dialer link;
    link.dialer.inject(data_frame("stray", 0xBAD));
    link.answer();
    REQUIRE(link.peer->is_complete());
    REQUIRE(link.dialer.subject->is_complete());

    link.publish("real");

    CHECK(link.dialer.subject->peer_session_id() == link.peer->session_id());
    CHECK(link.received == std::vector<std::string>{"real"});
    CHECK(link.dialer.fx.drops == std::vector{drop_cause::unestablished});
}

TEST_CASE("session gate latch: a frame with a foreign session id after completion leaves the session open", "[integration][session_gate]")
{
    answered_dialer link;
    link.answer();
    link.publish("real");
    const std::uint64_t latched = link.dialer.subject->peer_session_id();
    REQUIRE(latched == link.peer->session_id());
    REQUIRE(link.received == std::vector<std::string>{"real"});

    link.dialer.inject(data_frame("foreign", latched + 1));

    CHECK(link.dialer.subject->is_complete());
    CHECK(link.dialer.subject->peer_session_id() == latched);
    CHECK(link.received == std::vector<std::string>{"real"});
    CHECK(link.dialer.fx.drops == std::vector{drop_cause::foreign_session});
}

TEST_CASE("session gate latch: an engine observer counts a frame refused before establishment", "[integration][session_gate]")
{
    drop_recorder recorder;
    routing_inproc_fixture::two_node nodes;
    nodes.a.add_observer(recorder);
    routing_inproc_fixture::transport_t raw(nodes.ex, nodes.bus);
    std::unique_ptr<inproc_channel<engine_clock>> channel;
    raw.on_dialed([&channel](std::unique_ptr<inproc_channel<engine_clock>> ch, const plexus::io::endpoint &) { channel = std::move(ch); });
    raw.dial(nodes.ep_a);
    nodes.drive();
    REQUIRE(channel);

    channel->send(frame_for(wire::msg_type::heartbeat));
    nodes.drive();

    REQUIRE(recorder.seen.size() == 1);
    CHECK(recorder.seen.front().cause == drop_cause::unestablished);
    CHECK(nodes.a.session_for(recorder.seen.front().peer) != nullptr);
}

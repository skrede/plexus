#include "test_peer_session_inproc_lifecycle_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_inproc_fixture;

TEST_CASE("inproc peer_session: a handshake that never completes aborts once the timeout passes "
          "(virtual clock), looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto deadline = std::chrono::milliseconds(50);
        timeout_harness h(deadline);

        // Before the deadline: request sent, no response arrived — no abort.
        h.drive();
        REQUIRE(!h.requester.is_complete());

        // Cross the deadline and drive: on_timeout fires abort -> tear_down.
        manual_clock::advance(deadline + std::chrono::milliseconds(1));
        h.drive();
        REQUIRE(!h.requester.is_complete());
        REQUIRE(h.requester.peer_session_id() == 0); // epoch latch reset by teardown
    }
}

namespace {

// A manual-clock pair stood up through the transport rendezvous (no hand-dial) so the handshake
// CAN complete before the deadline.
struct manual_link
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    inproc_transport<manual_clock> transport;

    plexus::log::null_logger sink;
    manual_msg req_messages;
    manual_msg resp_messages;
    manual_rpc req_procedures;
    manual_rpc resp_procedures;

    plexus::io::peer_context<manual_policy> req_ctx;  // the dialer slot's per-peer record
    plexus::io::peer_context<manual_policy> resp_ctx; // the accepted slot's per-peer record
    std::optional<manual_session> requester;
    std::optional<manual_session> responder;

    explicit manual_link(std::chrono::nanoseconds timeout)
            : ex{bus}
            , transport{ex, bus}
            , req_messages{sink}
            , resp_messages{sink}
            , req_procedures{ex, std::chrono::hours(1), sink}
            , resp_procedures{ex, std::chrono::hours(1), sink}
    {
        transport.on_accepted(
                [this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, ex, make_cfg(0x01), timeout, resp_messages, resp_procedures, true, sink);
                    responder->start();
                });
        transport.on_dialed(
                [this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch, const plexus::io::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, ex, make_cfg(0x02), timeout, req_messages, req_procedures, false, sink);
                    requester->start();
                });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive()
    {
        ex.drain();
    }
};

}

TEST_CASE("inproc peer_session: completing before the deadline cancels the timer - no later abort, "
          "looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto deadline = std::chrono::milliseconds(50);
        manual_link l(deadline);

        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        // Advance well past the (now-canceled) deadline: NO abort fires, epoch intact.
        const auto epoch = l.requester->session_id();
        manual_clock::advance(deadline + std::chrono::seconds(10));
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.requester->session_id() == epoch);
    }
}

TEST_CASE("inproc peer_session: teardown drains the forwarders and resets the epoch latch", "[integration][peer_session][inproc]")
{
    session_link l;
    l.drive();
    REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
    l.drive();

    // Latch the responder's view of the requester's epoch with a real publish.
    l.req_messages.publish("topic", as_bytes(std::string{"x"}), l.requester->session_id());
    l.drive();
    REQUIRE(l.responder->peer_session_id() == l.requester->session_id());

    l.responder->tear_down();
    REQUIRE(!l.responder->is_complete());
    REQUIRE(l.responder->peer_session_id() == 0);

    // After teardown the responder no longer fans toward its peer (detach_all dropped the entry).
    l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "back");
    l.resp_messages.detach_all(l.responder->msg_peer());
    l.resp_messages.publish("back", as_bytes(std::string{"y"}), l.responder->session_id());
    l.drive();
    REQUIRE(l.req_received.empty());
}

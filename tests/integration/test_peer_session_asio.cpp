#include "test_peer_session_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_asio_fixture;

TEST_CASE("asio peer_session pair completes the handshake over real TCP and mints epochs, looped", "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    int completed              = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });

        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("asio peer_session: a dialed (one-directional) connection completes BOTH sides over real "
          "TCP - the accepted bootstrap responder answers, the dialer mints off the response, "
          "gated data flows both ways, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string downward = "dialer-to-responder-over-tcp";
    const std::string upward   = "responder-to-dialer-over-tcp";
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l; // the dial rendezvous: only the dialer dials, the accepted end bootstraps
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });

        // Both complete over real TCP without a simultaneous connect: the accepted
        // bootstrap responder sent its accept response over the socket, so the dialer
        // completed and minted its own epoch.
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach(l.requester->msg_peer(), "up"));
        REQUIRE(l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "up"));
        l.settle();

        l.req_messages.publish("down", as_bytes(downward), l.requester->session_id());
        l.resp_messages.publish("up", as_bytes(upward), l.responder->session_id());
        l.pump_until([&] { return !l.resp_received.empty() && !l.req_received.empty(); });

        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == downward);
        REQUIRE(l.req_received.size() == 1);
        REQUIRE(l.req_received[0] == upward);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        REQUIRE(l.requester->peer_session_id() == l.responder->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("asio peer_session: a real published message flows post-handshake over TCP and latches "
          "the epoch, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload  = "real-published-bytes-over-tcp";
    int delivered              = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        // The responder subscribes (so its forwarder resolves the topic_hash on the
        // receive tail); the requester fans the topic toward its peer, then publishes
        // carrying its minted epoch. The frame rides the live TCP channel.
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle(); // drain the subscribe handshake
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());

        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

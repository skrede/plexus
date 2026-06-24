#include "test_peer_session_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_inproc_fixture;

TEST_CASE("inproc peer_session pair completes the handshake, mints epochs, installs once, looped", "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        // A further drive does not re-mint or re-install (install-once latch).
        const auto req_epoch = l.requester->session_id();
        l.drive();
        REQUIRE(l.requester->session_id() == req_epoch);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("inproc peer_session: a dialed (one-directional) connection completes BOTH sides — the "
          "accepted bootstrap responder answers, the dialer mints off the response, and gated data "
          "flows both ways, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string downward     = "dialer-to-responder";
    const std::string upward       = "responder-to-dialer";
    int               proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l; // the dial rendezvous: only the dialer dials, the accepted end bootstraps
        l.drive();

        // Both complete WITHOUT a simultaneous connect: the accepted bootstrap responder
        // sent an accept response, so the dialer completed and minted its OWN epoch.
        // (Before the response-on-bootstrap-complete fix the dialer stranded and neither
        // flowed.)
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        // The established session is usable BOTH ways, each direction gated by the
        // sender's epoch (the receiver latches it).
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach(l.requester->msg_peer(), "up"));
        REQUIRE(l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "up"));
        l.drive();

        l.req_messages.publish("down", as_bytes(downward), l.requester->session_id());
        l.resp_messages.publish("up", as_bytes(upward), l.responder->session_id());
        l.drive();

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

TEST_CASE("inproc peer_session: a real published message flows post-handshake and latches the "
          "epoch, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string payload      = "real-published-bytes";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        // The responder subscribes (so its forwarder resolves the topic_hash on the
        // receive tail); the requester fans the topic toward its peer, then publishes
        // carrying its minted epoch. The frame rides the live channel to the responder.
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());
        l.drive();

        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

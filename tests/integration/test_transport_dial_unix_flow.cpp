#include "test_transport_dial_unix_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace transport_dial_unix_fixture;

TEST_CASE("unix transport: a real published message carrying the minted epoch flows post-dial over "
          "AF_UNIX, looped",
          "[integration][transport][unix]")
{
    constexpr int     k_iterations = 100;
    const std::string payload      = "dialed-published-bytes-over-unix";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_unix_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

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

#include "test_peer_session_inproc_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_inproc_fixture;

TEST_CASE("inproc peer_session: the opt-in 3-arg callback delivers a message_info with "
          "intra-process locality, looped",
          "[integration][peer_session][inproc][message_info]")
{
    constexpr int     k_iterations = 100;
    const std::string payload      = "info-bearing-bytes";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        // The 3-arg callback takes precedence over the 2-arg one set by the link ctor:
        // register it on the responder so the published frame's metadata reaches it.
        plexus::io::message_info got{};
        bool                     got_one = false;
        l.responder->on_message_with_info(
                [&](std::string_view, std::span<const std::byte> d, const plexus::io::message_info &mi)
                {
                    got     = mi;
                    got_one = true;
                    l.resp_received.emplace_back(to_string(d));
                });

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());
        l.drive();

        REQUIRE(got_one);
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        // The delivery rode a same-process inproc channel: from_intra_process is true,
        // derived from the channel's own endpoint scheme (never peer-supplied).
        CHECK(got.from_intra_process == true);
        // source_timestamp is the publisher's wire stamp; reception is receiver-stamped
        // and monotonic relative to it; the gid does not yet ride the wire.
        CHECK(got.source_timestamp != 0);
        CHECK(got.reception_timestamp >= got.source_timestamp);
        CHECK_FALSE(got.source_identity.has_value());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("inproc peer_session: a source-identity publish populates message_info.source_identity "
          "end-to-end, looped",
          "[integration][peer_session][inproc][message_info][gid]")
{
    // End-to-end through the real session deliver path: deliver_data reads the gid flag
    // from the live frame_header and reconstructs publisher_gid{ m_ctx.peer_id, counter }.
    // The DIALER (requester) receives — the canonical demand flow (a subscriber dials the
    // publisher), where peer_id is the true dialed peer id. The responder is the producer.
    constexpr int     k_iterations = 100;
    const std::string payload      = "attributed-bytes";
    const auto        producer_id  = make_cfg(0x01).self_id; // the responder's node_id
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        plexus::io::message_info got{};
        bool                     got_one = false;
        l.requester->on_message_with_info(
                [&](std::string_view, std::span<const std::byte> d, const plexus::io::message_info &mi)
                {
                    got     = mi;
                    got_one = true;
                    l.req_received.emplace_back(to_string(d));
                });

        // The responder is the producer: it declares source identity for the topic, fans
        // it toward the requester, and the requester subscribes.
        l.resp_messages.declare("topic", plexus::topic_qos{}, std::nullopt,
                                /*emit_source_identity=*/true);
        REQUIRE(l.req_messages.attach(l.requester->msg_peer(), "topic"));
        REQUIRE(l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "topic"));
        l.drive();
        l.resp_messages.publish("topic", as_bytes(payload), l.responder->session_id());
        l.drive();

        REQUIRE(got_one);
        REQUIRE(l.req_received.size() == 1);
        REQUIRE(l.req_received[0] == payload);
        REQUIRE(got.source_identity.has_value());
        // The node_id half is the PINNED session peer (the producer), NOT taken from the
        // frame; the counter half is the producer's minted endpoint counter.
        CHECK(got.source_identity->node_id() == producer_id);
        CHECK(got.source_identity->endpoint_counter() != 0);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("inproc peer_session: a real RPC round-trips post-handshake matched by correlation, looped", "[integration][peer_session][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string param        = "rpc-param";
    int               answered     = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        l.resp_procedures.serve("svc",
                                [](std::span<const std::byte> p, rpc_forwarder::reply_fn &reply)
                                {
                                    reply(rpc_status::success, p); // echo the param back
                                });

        int         fired = 0;
        rpc_status  got   = rpc_status::error;
        std::string ret;
        l.req_procedures.call(
                l.requester->rpc_peer(), "svc", as_bytes(param),
                [&](rpc_status s, std::span<const std::byte> r)
                {
                    ++fired;
                    got = s;
                    ret = to_string(r);
                },
                std::nullopt, l.requester->session_id());
        l.drive();

        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::success);
        REQUIRE(ret == param);
        ++answered;
    }
    REQUIRE(answered == k_iterations);
}

#include "test_peer_session_asio_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace peer_session_asio_fixture;

TEST_CASE("asio peer_session: the 3-arg callback reports NON-intra-process locality over real TCP, "
          "looped",
          "[integration][peer_session][asio][message_info]")
{
    constexpr int k_iterations = 100;
    const std::string payload  = "info-bearing-bytes-over-tcp";
    int delivered              = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        plexus::io::message_info got{};
        bool got_one = false;
        l.responder->on_message_with_info(
                [&](std::string_view, std::span<const std::byte> d, const plexus::io::message_info &mi)
                {
                    got     = mi;
                    got_one = true;
                    l.resp_received.emplace_back(to_string(d));
                });

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle();
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());

        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(got_one);
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        // The frame rode a real TCP channel: from_intra_process is false, derived from
        // the channel's own "tcp" endpoint scheme.
        CHECK(got.from_intra_process == false);
        CHECK(got.source_timestamp != 0);
        CHECK(got.reception_timestamp >= got.source_timestamp);
        CHECK_FALSE(got.source_identity.has_value());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("asio peer_session: the data-path staleness gate FIRES over TCP - a mismatched epoch is "
          "dropped, looped",
          "[integration][peer_session][asio]")
{
    constexpr int k_iterations = 100;
    const std::string good     = "latched-epoch-bytes";
    const std::string stale    = "stale-session-bytes";
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_link l;
        l.pump_until([&] { return l.requester && l.responder && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle();

        // A real publish over TCP latches the requester's epoch on the responder.
        l.req_messages.publish("topic", as_bytes(good), l.requester->session_id());
        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(l.resp_received.size() == 1);
        const auto latched = l.responder->peer_session_id();
        REQUIRE(latched == l.requester->session_id());

        // A frame for the SAME topic carrying a DIFFERENT non-zero epoch is dropped:
        // the sink does not grow. Fed through the production receive path. on_receive
        // gates synchronously (no post), so the drop is observable immediately — a
        // wall-clock wait would only risk a false green under load.
        const std::uint8_t stale_epoch = static_cast<std::uint8_t>(latched == 200 ? 199 : 200);
        auto stale_frame               = make_data_frame(stale, stale_epoch);
        l.responder->on_receive(stale_frame);
        REQUIRE(l.resp_received.size() == 1); // DROPPED, not delivered

        // A frame carrying the latched epoch IS delivered.
        auto fresh_frame = make_data_frame(good, latched);
        l.responder->on_receive(fresh_frame);
        l.pump_until([&] { return l.resp_received.size() >= 2; });
        REQUIRE(l.resp_received.size() == 2);
        REQUIRE(l.resp_received[1] == good);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

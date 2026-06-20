#include "test_frame_router_common.h"

using namespace frame_router_fixture;

TEST_CASE("frame_router dispatches each type to its registered consumer with the inner span",
          "[forwarder][router]")
{
    counting_logger log;
    frame_router    router(log);

    std::size_t uni = 0, sub = 0, unsub = 0, sub_resp = 0, rpc_req = 0, rpc_resp = 0;
    std::string last_inner;

    router.on_unidirectional(
            [&](const wire::frame_header &, std::span<const std::byte> in)
            {
                ++uni;
                last_inner = to_string(in);
            });
    router.on_subscribe(
            [&](std::span<const std::byte> in)
            {
                ++sub;
                last_inner = to_string(in);
            });
    router.on_unsubscribe(
            [&](std::span<const std::byte> in)
            {
                ++unsub;
                last_inner = to_string(in);
            });
    router.on_subscribe_response(
            [&](std::span<const std::byte> in)
            {
                ++sub_resp;
                last_inner = to_string(in);
            });
    router.on_rpc_request(
            [&](std::span<const std::byte> in)
            {
                ++rpc_req;
                last_inner = to_string(in);
            });
    router.on_rpc_response(
            [&](std::span<const std::byte> in)
            {
                ++rpc_resp;
                last_inner = to_string(in);
            });

    struct arm
    {
        wire::msg_type   type;
        std::string_view body;
        std::size_t     *hits;
    };
    const std::array<arm, 6> arms{
            {{wire::msg_type::unidirectional, "uni-inner", &uni},
             {wire::msg_type::subscribe, "sub-inner", &sub},
             {wire::msg_type::unsubscribe, "unsub-inner", &unsub},
             {wire::msg_type::subscribe_response, "sub-resp-inner", &sub_resp},
             {wire::msg_type::rpc_request, "rpc-req-inner", &rpc_req},
             {wire::msg_type::rpc_response, "rpc-resp-inner", &rpc_resp}}};

    for(const auto &a : arms)
    {
        auto       frame  = make_frame(a.type, a.body);
        const auto before = *a.hits;
        router.route(frame);
        REQUIRE(*a.hits == before + 1); // exactly the matching consumer fired
        REQUIRE(last_inner == a.body);  // with the correct inner (header-off) span
    }

    // Each consumer fired exactly once; none cross-fired.
    REQUIRE((uni == 1 && sub == 1 && unsub == 1 && sub_resp == 1 && rpc_req == 1 && rpc_resp == 1));
    REQUIRE(log.count == 0); // no drop on the happy path
}

TEST_CASE("frame_router delivers a handshake_req / handshake_resp to a registered consumer",
          "[forwarder][router]")
{
    counting_logger log;
    frame_router    router(log);

    std::size_t hs_req = 0, hs_resp = 0;
    std::string last_inner;
    router.on_handshake_req(
            [&](std::span<const std::byte> in)
            {
                ++hs_req;
                last_inner = to_string(in);
            });
    router.on_handshake_resp(
            [&](std::span<const std::byte> in)
            {
                ++hs_resp;
                last_inner = to_string(in);
            });

    router.route(make_frame(wire::msg_type::handshake_req, "hs-req-inner"));
    REQUIRE(hs_req == 1);
    REQUIRE(last_inner == "hs-req-inner");

    router.route(make_frame(wire::msg_type::handshake_resp, "hs-resp-inner"));
    REQUIRE(hs_resp == 1);
    REQUIRE(last_inner == "hs-resp-inner");

    REQUIRE(log.count == 0); // no drop on the happy path
}

TEST_CASE("frame_router warn-and-drops a handshake frame with no consumer", "[forwarder][router]")
{
    counting_logger log;
    frame_router    router(log); // no handshake consumers registered

    router.route(make_frame(wire::msg_type::handshake_req, "hs-req-inner"));
    router.route(make_frame(wire::msg_type::handshake_resp, "hs-resp-inner"));

    REQUIRE(log.count == 2); // both warn-and-dropped: no consumer for the type
}

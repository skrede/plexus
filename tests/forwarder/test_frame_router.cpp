#include "plexus/io/frame_router.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::io::frame_router;
namespace wire = plexus::wire;

namespace {

// A test logger whose warn() bumps a counter — proves the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view) override { ++count; }
    std::size_t count{0};
};

// Builds a complete (header-on) frame of the given type carrying body as the
// inner payload — exactly the shape both backends post to on_data.
std::vector<std::byte> make_frame(wire::msg_type type, std::string_view body)
{
    std::vector<std::byte> inner(body.size());
    for(std::size_t i = 0; i < body.size(); ++i)
        inner[i] = static_cast<std::byte>(static_cast<unsigned char>(body[i]));

    wire::frame_header hdr{
            .type         = type,
            .flags        = 0,
            .session_id   = 0,
            .timestamp_ns = 0,
            .payload_len  = inner.size()
    };
    return wire::encode_frame(hdr, inner);
}

std::string to_string(std::span<const std::byte> s)
{
    return std::string(reinterpret_cast<const char *>(s.data()), s.size());
}

}

TEST_CASE("frame_router dispatches each type to its registered consumer with the inner span", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);

    std::size_t uni = 0, sub = 0, unsub = 0, sub_resp = 0, rpc_req = 0, rpc_resp = 0;
    std::string last_inner;

    router.on_unidirectional([&](const wire::frame_header &, std::span<const std::byte> in) { ++uni; last_inner = to_string(in); });
    router.on_subscribe([&](std::span<const std::byte> in) { ++sub; last_inner = to_string(in); });
    router.on_unsubscribe([&](std::span<const std::byte> in) { ++unsub; last_inner = to_string(in); });
    router.on_subscribe_response([&](std::span<const std::byte> in) { ++sub_resp; last_inner = to_string(in); });
    router.on_rpc_request([&](std::span<const std::byte> in) { ++rpc_req; last_inner = to_string(in); });
    router.on_rpc_response([&](std::span<const std::byte> in) { ++rpc_resp; last_inner = to_string(in); });

    struct arm { wire::msg_type type; std::string_view body; std::size_t *hits; };
    const std::array<arm, 6> arms{{
            {wire::msg_type::unidirectional,     "uni-inner",      &uni},
            {wire::msg_type::subscribe,          "sub-inner",      &sub},
            {wire::msg_type::unsubscribe,        "unsub-inner",    &unsub},
            {wire::msg_type::subscribe_response, "sub-resp-inner", &sub_resp},
            {wire::msg_type::rpc_request,        "rpc-req-inner",  &rpc_req},
            {wire::msg_type::rpc_response,       "rpc-resp-inner", &rpc_resp}
    }};

    for(const auto &a : arms)
    {
        auto frame = make_frame(a.type, a.body);
        const auto before = *a.hits;
        router.route(frame);
        REQUIRE(*a.hits == before + 1);     // exactly the matching consumer fired
        REQUIRE(last_inner == a.body);      // with the correct inner (header-off) span
    }

    // Each consumer fired exactly once; none cross-fired.
    REQUIRE((uni == 1 && sub == 1 && unsub == 1 && sub_resp == 1 && rpc_req == 1 && rpc_resp == 1));
    REQUIRE(log.count == 0);                // no drop on the happy path
}

TEST_CASE("frame_router delivers a handshake_req / handshake_resp to a registered consumer", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);

    std::size_t hs_req = 0, hs_resp = 0;
    std::string last_inner;
    router.on_handshake_req([&](std::span<const std::byte> in) { ++hs_req; last_inner = to_string(in); });
    router.on_handshake_resp([&](std::span<const std::byte> in) { ++hs_resp; last_inner = to_string(in); });

    router.route(make_frame(wire::msg_type::handshake_req, "hs-req-inner"));
    REQUIRE(hs_req == 1);
    REQUIRE(last_inner == "hs-req-inner");

    router.route(make_frame(wire::msg_type::handshake_resp, "hs-resp-inner"));
    REQUIRE(hs_resp == 1);
    REQUIRE(last_inner == "hs-resp-inner");

    REQUIRE(log.count == 0);   // no drop on the happy path
}

TEST_CASE("frame_router warn-and-drops a handshake frame with no consumer", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);   // no handshake consumers registered

    router.route(make_frame(wire::msg_type::handshake_req, "hs-req-inner"));
    router.route(make_frame(wire::msg_type::handshake_resp, "hs-resp-inner"));

    REQUIRE(log.count == 2);   // both warn-and-dropped: no consumer for the type
}

TEST_CASE("frame_router warn-and-drops a short frame", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);

    bool fired = false;
    router.on_subscribe([&](std::span<const std::byte>) { fired = true; });

    std::vector<std::byte> short_frame(wire::header_size - 1, std::byte{0x00});
    router.route(short_frame);

    REQUIRE_FALSE(fired);
    REQUIRE(log.count == 1);
}

TEST_CASE("frame_router warn-and-drops a bad-magic frame", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);

    bool fired = false;
    router.on_subscribe([&](std::span<const std::byte>) { fired = true; });

    // header_size bytes that fail the magic check (no 0x56 0x50 prefix).
    std::vector<std::byte> bad(wire::header_size + 4, std::byte{0xAB});
    router.route(bad);

    REQUIRE_FALSE(fired);
    REQUIRE(log.count == 1);
}

TEST_CASE("frame_router warn-and-drops an unregistered type", "[forwarder][router]")
{
    counting_logger log;
    frame_router router(log);   // no consumers registered

    auto frame = make_frame(wire::msg_type::subscribe, "sub-inner");
    router.route(frame);        // valid frame, but no subscribe consumer

    REQUIRE(log.count == 1);    // warn-and-dropped: no consumer for the type
}

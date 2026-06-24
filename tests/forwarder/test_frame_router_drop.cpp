#include "test_frame_router_common.h"

using namespace frame_router_fixture;

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
    frame_router router(log); // no consumers registered

    auto frame = make_frame(wire::msg_type::subscribe, "sub-inner");
    router.route(frame); // valid frame, but no subscribe consumer

    REQUIRE(log.count == 1); // warn-and-dropped: no consumer for the type
}

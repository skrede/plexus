#include "test_handles_common.h"

using namespace handles_fixture;

TEST_CASE("handles: a publisher is move-only", "[node][handles]")
{
    static_assert(!std::is_copy_constructible_v<inproc_publisher>);
    static_assert(!std::is_copy_assignable_v<inproc_publisher>);
    static_assert(std::is_nothrow_move_constructible_v<inproc_publisher>);
    static_assert(std::is_nothrow_move_assignable_v<inproc_publisher>);
}

TEST_CASE("handles: a subscriber is move-only", "[node][handles]")
{
    static_assert(!std::is_copy_constructible_v<inproc_subscriber>);
    static_assert(!std::is_copy_assignable_v<inproc_subscriber>);
    static_assert(std::is_nothrow_move_constructible_v<inproc_subscriber>);
    static_assert(std::is_nothrow_move_assignable_v<inproc_subscriber>);
}

TEST_CASE("handles: moving a subscriber transfers the demand; the moved-from dtor is a no-op",
          "[node][handles]")
{
    net                      n;
    std::vector<std::string> got;

    {
        inproc_subscriber s1{n.a, "topic",
                             [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        n.drive();

        // Move the live demand into s2, then double-move into s3. The moved-from s1/s2
        // dtors must NOT retire the demand — only s3's does.
        inproc_subscriber s2{std::move(s1)};
        inproc_subscriber s3{std::move(s2)};

        inproc_publisher p{n.b, "topic"};
        n.drive();
        p.publish(as_bytes("payload"));
        n.drive();

        REQUIRE(got.size() == 1);
        REQUIRE(got.front() == "payload");
    }
    // s3 went out of scope: the demand is retired (last subscriber for the fqn gone).
    REQUIRE(n.a_demand_for("topic") == 0);
}

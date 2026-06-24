#include "test_handles_common.h"

using namespace handles_fixture;

TEST_CASE("handles: dropping a subscriber stops its callback and retires the engine demand", "[node][handles]")
{
    net                      n;
    std::vector<std::string> got;
    inproc_publisher         p{n.b, "topic"};

    {
        inproc_subscriber s{n.a, "topic", [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
        n.drive();
        REQUIRE(n.a_demand_for("topic") == 1);

        p.publish(as_bytes("first"));
        n.drive();
        REQUIRE(got.size() == 1);
    }

    // The subscriber is gone: its demand is retired and a further publish never fires it.
    REQUIRE(n.a_demand_for("topic") == 0);
    p.publish(as_bytes("after-drop"));
    n.drive();
    REQUIRE(got.size() == 1); // still 1 — no callback after drop
}

TEST_CASE("handles: two subscribers on one fqn both fire and retire independently", "[node][handles]")
{
    net                      n;
    std::vector<std::string> got1;
    std::vector<std::string> got2;
    inproc_publisher         p{n.b, "topic"};

    auto s1 = inproc_subscriber{n.a, "topic", [&](std::span<const std::byte> b) { got1.push_back(to_string(b)); }};
    {
        inproc_subscriber s2{n.a, "topic", [&](std::span<const std::byte> b) { got2.push_back(to_string(b)); }};
        n.drive();

        p.publish(as_bytes("both"));
        n.drive();
        REQUIRE(got1.size() == 1);
        REQUIRE(got2.size() == 1);

        // The engine demand is still live while ANY local subscriber holds the fqn.
        REQUIRE(n.a_demand_for("topic") == 1);
    }
    // s2 retired, but s1 keeps the fqn alive: the demand persists and s1 still fires.
    REQUIRE(n.a_demand_for("topic") == 1);
    p.publish(as_bytes("only-s1"));
    n.drive();
    REQUIRE(got1.size() == 2);
    REQUIRE(got2.size() == 1);
}

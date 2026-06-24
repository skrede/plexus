#include "test_history_ring_common.h"

using namespace history_ring_fixture;

TEST_CASE("durability=all with replay_depth caps to the most-recent replay_depth frames", "[history_ring][forwarder]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder fwd{sink};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 5; ++i)
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::all;
        q.replay_depth    = 2;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 2);                             // min(count=5, replay_depth=2)
        REQUIRE(bodies == std::vector<std::string>{"v3", "v4"}); // the newest 2, oldest->newest
    }
}

TEST_CASE("a depth-1 ring stays byte-identical to last-writer-wins (all/latest/none)", "[history_ring][forwarder]")
{
    auto replay = [](durability mode)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        plexus::log::null_logger sink;
        forwarder fwd{sink};
        fwd.latch("topic"); // depth-1 convenience
        fwd.publish("topic", as_bytes(std::string{"v1"}));
        fwd.publish("topic", as_bytes(std::string{"v2"}));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = mode;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();
        return data_bodies(cap);
    };

    SECTION("all -> exactly the single newest (last-writer-wins)")
    {
        auto bodies = replay(durability::all);
        REQUIRE(bodies.size() == 1);
        REQUIRE(bodies[0] == "v2");
    }
    SECTION("latest -> exactly the single newest")
    {
        auto bodies = replay(durability::latest);
        REQUIRE(bodies.size() == 1);
        REQUIRE(bodies[0] == "v2");
    }
    SECTION("none -> zero")
    {
        REQUIRE(replay(durability::none).empty());
    }
}

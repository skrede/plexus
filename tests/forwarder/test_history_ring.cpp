#include "test_history_ring_common.h"

using namespace history_ring_fixture;

TEST_CASE("durability=all on a depth-N ring replays EXACTLY the last N oldest->newest",
          "[history_ring][forwarder]")
{
    // Reproducibility: the exactly-N delivery effect is proven over repeated runs.
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        forwarder fwd{};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 7; ++i) // N+k: publish v0..v6 with zero subscribers
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::all;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 5); // the last N, not N+k, not 1
        REQUIRE(bodies == std::vector<std::string>{"v2", "v3", "v4", "v5", "v6"});
    }
}

TEST_CASE("durability=all with fewer than N retained replays all of them oldest->newest",
          "[history_ring][forwarder]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        forwarder fwd{};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 3; ++i)
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::all;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 3); // count < N
        REQUIRE(bodies == std::vector<std::string>{"v0", "v1", "v2"});
    }
}

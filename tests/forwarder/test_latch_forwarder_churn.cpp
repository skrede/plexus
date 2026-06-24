#include "test_latch_forwarder_common.h"

using namespace latch_forwarder_fixture;

TEST_CASE("latch replay targets only the new subscriber, no double-receive", "[latch][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch_a(ex), ch_b(ex);
    capture cap_a(ex), cap_b(ex);
    auto peer_a = make_peer(ch_a, cap_a, "node-a");
    auto peer_b = make_peer(ch_b, cap_b, "node-b");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
    fwd.latch("topic");
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic")); // A before the publish
    ex.drain();
    cap_a.frames.clear();

    fwd.publish("topic", as_bytes(std::string{"the-value"})); // A gets it live, once
    ex.drain();

    // B (the late joiner) must declare durability::latest — only that durability gets the replay.
    plexus::io::subscriber_qos sub_qos_b;
    sub_qos_b.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer_b, "topic", std::nullopt,
                                  sub_qos_b)); // B after — gets the replay
    ex.drain();

    REQUIRE(data_bodies(cap_a).size() == 1); // A: exactly the one live frame, no replay
    auto bodies_b = data_bodies(cap_b);
    REQUIRE(bodies_b.size() == 1);
    REQUIRE(bodies_b[0] == "the-value");
}

TEST_CASE("latch retention survives subscriber churn", "[latch][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch_a(ex);
    capture cap_a(ex);
    auto peer_a = make_peer(ch_a, cap_a, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"survivor"}));
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic"));
    ex.drain();
    fwd.detach_all(peer_a); // the last subscriber leaves

    inproc_channel<> ch_b(ex);
    capture cap_b(ex);
    auto peer_b = make_peer(ch_b, cap_b, "node-b");
    // The new late joiner must declare durability::latest — only that durability gets the replay.
    plexus::io::subscriber_qos sub_qos_b;
    sub_qos_b.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer_b, "topic", std::nullopt, sub_qos_b)); // a new late joiner
    ex.drain();

    auto bodies = data_bodies(cap_b);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "survivor"); // the value outlived the churn
}

TEST_CASE("multi-publisher to one latched topic is last-writer-wins", "[latch][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"writer-1"}));
    fwd.publish("topic", as_bytes(std::string{"writer-2"}));
    ex.drain();

    // The subscriber must declare durability::latest — only that durability gets the replay.
    plexus::io::subscriber_qos sub_qos;
    sub_qos.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, sub_qos));
    ex.drain();
    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "writer-2"); // one slot per topic_hash, last write retained
}

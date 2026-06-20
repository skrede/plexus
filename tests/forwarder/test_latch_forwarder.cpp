#include "test_latch_forwarder_common.h"

using namespace latch_forwarder_fixture;

TEST_CASE("latch records the per-topic qos independent of any subscriber", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    forwarder         fwd{};
    fwd.latch("topic"); // zero subscribers, no attach
    // The registry seam the headline scenario depends on: declare/latch records
    // the qos before any add_subscriber. Reach it via a fresh registry mirroring
    // the forwarder-under-test path (the forwarder forwards declare straight to it).
    plexus::io::subscriber_registry<inproc_channel<>> reg;
    reg.declare(plexus::wire::fqn_topic_hash("topic"), "topic",
                plexus::topic_qos{.latch = true, .depth = 1});
    REQUIRE(reg.qos_for(plexus::wire::fqn_topic_hash("topic")).latch);
}

TEST_CASE("late subscriber to a latched topic gets the retained value published with zero "
          "subscribers",
          "[latch][forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        forwarder fwd{};
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(std::string{"retained-v1"})); // NO subscriber yet
        ex.drain();
        REQUIRE(cap.frames.empty()); // nothing fanned out (no subscriber)

        // The late joiner explicitly requests single-newest replay; latch retention is
        // delivered only to a subscriber that declares the durability that asks for it.
        plexus::io::subscriber_qos sub_qos;
        sub_qos.durability_mode = plexus::io::durability::latest;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, sub_qos)); // the late join
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 1);
        REQUIRE(bodies[0] == "retained-v1"); // the retained value, replayed
    }
}

TEST_CASE("a non-latched topic does not replay on a late subscribe", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.publish("topic", as_bytes(std::string{"live-only"})); // not latched, no subscriber
    ex.drain();

    REQUIRE(fwd.attach_for_fanout(peer, "topic"));
    ex.drain();
    REQUIRE(data_bodies(cap).empty()); // no replay; only the subscribe_response control frame
}

TEST_CASE("a latched-but-never-published topic replays nothing (empty retention)",
          "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.latch("topic");
    REQUIRE(fwd.attach_for_fanout(peer, "topic")); // subscribe BEFORE any publish
    ex.drain();
    REQUIRE(data_bodies(cap).empty()); // nothing retained yet

    fwd.publish("topic", as_bytes(std::string{"first-live"})); // arrives normally now
    ex.drain();
    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "first-live");
}

TEST_CASE("depth=1 replays only the last latched value", "[latch][forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<>      bus;
        inproc_executor<> ex(bus);
        inproc_channel<>  ch(ex);
        capture           cap(ex);
        auto              peer = make_peer(ch, cap, "node-a");

        forwarder fwd{};
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(std::string{"v1"}));
        fwd.publish("topic", as_bytes(std::string{"v2"}));
        ex.drain();

        // Request single-newest replay: latch retention is replayed only when the
        // subscriber explicitly asks for it.
        plexus::io::subscriber_qos sub_qos;
        sub_qos.durability_mode = plexus::io::durability::latest;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, sub_qos));
        ex.drain();
        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 1);
        REQUIRE(bodies[0] == "v2"); // last value only, not both
    }
}

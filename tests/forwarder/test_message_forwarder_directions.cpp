#include "test_message_forwarder_common.h"

#include "plexus/io/subscriber_qos.h"

using namespace message_forwarder_fixture;

namespace {

struct one_peer
{
    one_peer()
            : ex(bus)
            , ch(ex)
            , cap(ex)
            , peer(make_peer(ch, cap, "node-a"))
            , fwd(sink)
    {
    }

    inproc_bus<> bus;
    inproc_executor<> ex;
    inproc_channel<> ch;
    capture cap;
    forwarder::peer peer;
    plexus::log::null_logger sink;
    forwarder fwd;
};

}

TEST_CASE("subscription_directions a peer's subscribe is answered while this node subscribes to the same topic on that peer", "[forwarder]")
{
    one_peer t;
    REQUIRE(t.fwd.attach(t.peer, "alpha"));
    REQUIRE(t.fwd.attach_for_fanout(t.peer, "alpha"));
    t.ex.drain();
    REQUIRE(count_subscribe_responses(t.cap) == 1);
}

TEST_CASE("subscription_directions this node's subscribe goes out while the peer subscribes to the same topic here", "[forwarder]")
{
    one_peer t;
    REQUIRE(t.fwd.attach_for_fanout(t.peer, "alpha"));
    REQUIRE(t.fwd.attach(t.peer, "alpha"));
    t.ex.drain();
    REQUIRE(count_subscribes(t.cap) == 1);
}

TEST_CASE("subscription_directions this node's unsubscribe goes out while the peer still subscribes here", "[forwarder]")
{
    one_peer t;
    t.fwd.attach(t.peer, "alpha");
    t.fwd.attach_for_fanout(t.peer, "alpha");
    t.ex.drain();
    t.cap.frames.clear();
    REQUIRE(t.fwd.detach(t.peer, "alpha"));
    t.ex.drain();
    REQUIRE(count_unsubscribes(t.cap) == 1);
    t.fwd.publish("alpha", as_bytes(std::string{"after-own-unsubscribe"}));
    t.ex.drain();
    REQUIRE(count_data_frames(t.cap) == 1);
}

TEST_CASE("subscription_directions the peer's unsubscribe leaves this node's subscription to that peer", "[forwarder]")
{
    one_peer t;
    t.fwd.attach(t.peer, "alpha");
    t.fwd.attach_for_fanout(t.peer, "alpha");
    REQUIRE(t.fwd.detach_for_fanout(t.peer, "alpha"));
    t.ex.drain();
    t.cap.frames.clear();
    t.fwd.publish("alpha", as_bytes(std::string{"after-peer-unsubscribe"}));
    t.ex.drain();
    REQUIRE(count_data_frames(t.cap) == 0);
    REQUIRE(t.fwd.detach(t.peer, "alpha"));
    t.ex.drain();
    REQUIRE(count_unsubscribes(t.cap) == 1);
}

TEST_CASE("subscription_directions subscribing to a peer does not fan this node's publications to it", "[forwarder]")
{
    one_peer t;
    t.fwd.attach(t.peer, "alpha");
    t.ex.drain();
    t.cap.frames.clear();
    t.fwd.publish("alpha", as_bytes(std::string{"unsolicited"}));
    t.ex.drain();
    REQUIRE(count_data_frames(t.cap) == 0);
}

TEST_CASE("subscription_directions a peer admitted while this node subscribes to it keeps its own qos", "[forwarder]")
{
    one_peer t;
    t.fwd.latch("alpha");
    t.fwd.publish("alpha", as_bytes(std::string{"retained"}));
    t.ex.drain();
    t.fwd.attach(t.peer, "alpha");
    t.ex.drain();
    t.cap.frames.clear();
    plexus::io::subscriber_qos latest;
    latest.durability_mode = plexus::io::durability::latest;
    REQUIRE(t.fwd.attach_for_fanout(t.peer, "alpha", std::nullopt, latest));
    t.ex.drain();
    REQUIRE(count_data_frames(t.cap) == 1);
}

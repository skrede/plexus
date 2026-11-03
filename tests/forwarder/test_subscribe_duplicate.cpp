#include "test_message_forwarder_common.h"

#include "plexus/topic_qos.h"

#include "plexus/io/subscriber_qos.h"
#include "plexus/io/demand_transition.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstddef>
#include <optional>
#include <string_view>

using namespace message_forwarder_fixture;
using plexus::io::demand_role;
using plexus::io::demand_transition;

namespace {

// Declaration order is construction order: the executor needs the bus, the peer needs the capture.
struct duplicate_fixture
{
    duplicate_fixture()
            : ex(bus)
            , ch(ex)
            , cap(ex)
            , fwd(sink)
            , peer(make_peer(ch, cap, "node-a"))
            , demand_ups(0)
    {
        fwd.on_demand_transition(
                [this](std::string_view, std::string_view, demand_transition dir, demand_role role)
                {
                    if(dir == demand_transition::up && role == demand_role::publisher)
                        ++demand_ups;
                });
    }

    inproc_bus<> bus;
    inproc_executor<> ex;
    inproc_channel<> ch;
    capture cap;
    plexus::log::null_logger sink;
    forwarder fwd;
    forwarder::peer peer;
    std::size_t demand_ups;
};

plexus::io::subscriber_qos latest()
{
    plexus::io::subscriber_qos q;
    q.durability_mode = plexus::io::durability::latest;
    return q;
}

}

TEST_CASE("subscribe duplicate: a repeated subscribe on one channel is answered", "[forwarder][subscribe_duplicate]")
{
    duplicate_fixture f;
    REQUIRE(f.fwd.attach_for_fanout(f.peer, "alpha"));
    CHECK(f.fwd.attach_for_fanout(f.peer, "alpha"));
    f.ex.drain();

    CHECK(count_subscribe_responses(f.cap) == 2);
}

TEST_CASE("subscribe duplicate: a repeated subscribe adds no fan-out, replay or demand edge", "[forwarder][subscribe_duplicate]")
{
    duplicate_fixture f;
    f.fwd.declare("alpha", plexus::topic_qos{.latch = true, .depth = 1});
    f.fwd.publish("alpha", as_bytes(std::string{"retained"}));
    f.fwd.attach_for_fanout(f.peer, "alpha", std::nullopt, latest());
    f.fwd.attach_for_fanout(f.peer, "alpha", std::nullopt, latest());
    f.ex.drain();
    CHECK(count_data_frames(f.cap) == 1);

    f.fwd.publish("alpha", as_bytes(std::string{"live"}));
    f.ex.drain();
    CHECK(count_data_frames(f.cap) == 2);
    CHECK(f.demand_ups == 1);
}

TEST_CASE("subscribe duplicate: one unsubscribe after a repeated subscribe detaches the channel", "[forwarder][subscribe_duplicate]")
{
    duplicate_fixture f;
    f.fwd.attach_for_fanout(f.peer, "alpha");
    f.fwd.attach_for_fanout(f.peer, "alpha");
    CHECK(f.fwd.detach_for_fanout(f.peer, "alpha"));
    f.ex.drain();
    f.cap.frames.clear();

    f.fwd.publish("alpha", as_bytes(std::string{"after"}));
    f.ex.drain();
    CHECK(count_data_frames(f.cap) == 0);
}

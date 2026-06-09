#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/topic_qos.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>
#include <optional>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::io::subscriber_qos;
using plexus::io::durability;
using plexus::topic_qos;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

struct capture
{
    explicit capture(inproc_executor<> &ex)
        : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d) {
            frames.emplace_back(d.begin(), d.end());
        });
    }

    inproc_channel<> sink;
    std::vector<std::vector<std::byte>> frames;
};

forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

std::vector<std::string> data_bodies(const capture &cap)
{
    std::vector<std::string> bodies;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::unidirectional)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        auto decoded = plexus::wire::decode_unidirectional(inner);
        if(!decoded)
            continue;
        bodies.emplace_back(reinterpret_cast<const char *>(decoded->data.data()),
                            decoded->data.size());
    }
    return bodies;
}

}

TEST_CASE("durability=all on a depth-N ring replays EXACTLY the last N oldest->newest",
          "[history_ring][forwarder]")
{
    // Reproducibility: the exactly-N delivery effect is proven over repeated runs.
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 7; ++i)   // N+k: publish v0..v6 with zero subscribers
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::all;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 5);   // the last N, not N+k, not 1
        REQUIRE(bodies == std::vector<std::string>{"v2", "v3", "v4", "v5", "v6"});
    }
}

TEST_CASE("durability=all with fewer than N retained replays all of them oldest->newest",
          "[history_ring][forwarder]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 3; ++i)
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::all;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 3);   // count < N
        REQUIRE(bodies == std::vector<std::string>{"v0", "v1", "v2"});
    }
}

TEST_CASE("durability=all with replay_depth caps to the most-recent replay_depth frames",
          "[history_ring][forwarder]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
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
        REQUIRE(bodies.size() == 2);   // min(count=5, replay_depth=2)
        REQUIRE(bodies == std::vector<std::string>{"v3", "v4"});   // the newest 2, oldest->newest
    }
}

TEST_CASE("a depth-1 ring stays byte-identical to last-writer-wins (all/latest/none)",
          "[history_ring][forwarder]")
{
    auto replay = [](durability mode) {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
        fwd.latch("topic");   // depth-1 convenience
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

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
using plexus::io::delivery;
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

TEST_CASE("a none+pull subscriber gets 0 on subscribe, then fetch_latched caps at min(max,count)",
          "[fetch_latched][forwarder]")
{
    // Reproducibility: the PULL delivery effects are proven over repeated runs.
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");
        // The topic_hash the wire fetch_latched_request would carry.
        const auto hash = plexus::wire::fqn_topic_hash("topic");

        forwarder fwd{};
        fwd.declare("topic", topic_qos{.latch = true, .depth = 5});
        for(int i = 0; i < 5; ++i)
            fwd.publish("topic", as_bytes("v" + std::to_string(i)));
        ex.drain();

        // none + pull: durability=none gates the push, so the subscribe replays nothing.
        subscriber_qos q;
        q.durability_mode = durability::none;
        q.delivery_mode   = delivery::pull;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();
        REQUIRE(data_bodies(cap).empty());

        // fetch(3) -> the newest 3, oldest->newest.
        cap.frames.clear();
        fwd.fetch_latched(peer, hash, 3);
        ex.drain();
        REQUIRE(data_bodies(cap) == std::vector<std::string>{"v2", "v3", "v4"});

        // fetch(10) -> capped at count (5), oldest->newest.
        cap.frames.clear();
        fwd.fetch_latched(peer, hash, 10);
        ex.drain();
        REQUIRE(data_bodies(cap) == std::vector<std::string>{"v0", "v1", "v2", "v3", "v4"});
    }
}

TEST_CASE("fetch_latched against a never-declared topic replays zero frames (no crash)",
          "[fetch_latched][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.fetch_latched(peer, plexus::wire::fqn_topic_hash("nope"), 5);
    ex.drain();
    REQUIRE(data_bodies(cap).empty());
}

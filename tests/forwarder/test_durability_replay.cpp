#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
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
#include <type_traits>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::io::subscriber_qos;
using plexus::io::durability;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

static_assert(std::is_trivially_copyable_v<subscriber_qos>);

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

TEST_CASE("a durability=none subscriber gets ZERO retained frames on subscribe",
          "[durability][forwarder]")
{
    // Reproducibility: a delivery-effect feature is proven over repeated runs.
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(std::string{"retained-v1"}));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::none;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        REQUIRE(data_bodies(cap).empty());   // none -> no retained frame
    }
}

TEST_CASE("a durability=latest subscriber gets EXACTLY ONE retained frame",
          "[durability][forwarder]")
{
    for(int iter = 0; iter < 50; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd{ex};
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(std::string{"retained-v1"}));
        ex.drain();

        subscriber_qos q;
        q.durability_mode = durability::latest;
        REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
        ex.drain();

        auto bodies = data_bodies(cap);
        REQUIRE(bodies.size() == 1);
        REQUIRE(bodies[0] == "retained-v1");
    }
}

TEST_CASE("a default-qos subscriber gets EXACTLY ONE retained frame (default == latest)",
          "[durability][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd{ex};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"retained-v1"}));
    ex.drain();

    // No qos argument: the friendly default IS latest, so un-upgraded subscribers
    // preserve today's single-frame latch replay.
    REQUIRE(fwd.attach_for_fanout(peer, "topic"));
    ex.drain();

    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "retained-v1");
}

TEST_CASE("a durability=all subscriber replays as latest against the depth-1 slot",
          "[durability][forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd{ex};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"retained-v1"}));
    ex.drain();

    subscriber_qos q;
    q.durability_mode = durability::all;
    REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, q));
    ex.drain();

    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "retained-v1");
}

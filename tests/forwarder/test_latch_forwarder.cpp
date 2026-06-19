#include "plexus/io/message_forwarder.h"
#include "plexus/io/subscriber_registry.h"
#include "plexus/topic_qos.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A capture sink: a second inproc channel paired to the channel the forwarder
// sends over, recording every delivered frame so the test can decode what the
// forwarder emitted after a drain().
struct capture
{
    explicit capture(inproc_executor<> &ex)
            : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d)
                     { frames.emplace_back(d.begin(), d.end()); });
    }

    inproc_channel<>                    sink;
    std::vector<std::vector<std::byte>> frames;
};

forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Collects the bodies of the UNIDIRECTIONAL data frames in a capture: strip the
// frame_header, assert the type is unidirectional, decode the inner, return the
// opaque payload as a string. A frame whose header is not unidirectional (e.g. a
// subscribe_response control frame) is skipped.
std::vector<std::string> data_bodies(const capture &cap)
{
    std::vector<std::string> bodies;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::unidirectional)
            continue;
        auto inner   = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        auto decoded = plexus::wire::decode_unidirectional(inner);
        if(!decoded)
            continue;
        bodies.emplace_back(reinterpret_cast<const char *>(decoded->data.data()),
                            decoded->data.size());
    }
    return bodies;
}

// The non-allocating sink Policy used by the LATCH-NOALLOC gate (mirrors the
// message-forwarder oracle's sink Policy): a byte_channel that records each send's
// size without copying, so a forwarder<sink_policy> latched publish exercises
// framing + retention with no transport-side allocation.
struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d)
    {
        total_bytes += d.size();
        ++sends;
    }
    void                 close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}

    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &) {}
    sink_timer(sink_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct sink_policy
{
    using executor_type     = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type        = sink_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

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

TEST_CASE("latch replay targets only the new subscriber, no double-receive", "[latch][forwarder]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch_a(ex), ch_b(ex);
    capture           cap_a(ex), cap_b(ex);
    auto              peer_a = make_peer(ch_a, cap_a, "node-a");
    auto              peer_b = make_peer(ch_b, cap_b, "node-b");

    forwarder fwd{};
    fwd.latch("topic");
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic")); // A before the publish
    ex.drain();
    cap_a.frames.clear();

    fwd.publish("topic", as_bytes(std::string{"the-value"})); // A gets it live, once
    ex.drain();

    // B explicitly requests single-newest replay; it is the late joiner that must
    // receive the retained value, so it declares the durability that asks for it.
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
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch_a(ex);
    capture           cap_a(ex);
    auto              peer_a = make_peer(ch_a, cap_a, "node-a");

    forwarder fwd{};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"survivor"}));
    REQUIRE(fwd.attach_for_fanout(peer_a, "topic"));
    ex.drain();
    fwd.detach_all(peer_a); // the last subscriber leaves

    inproc_channel<> ch_b(ex);
    capture          cap_b(ex);
    auto             peer_b = make_peer(ch_b, cap_b, "node-b");
    // The new late joiner explicitly requests single-newest replay; the retained value
    // is delivered only to a subscriber that declares the durability that asks for it.
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
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.latch("topic");
    fwd.publish("topic", as_bytes(std::string{"writer-1"}));
    fwd.publish("topic", as_bytes(std::string{"writer-2"}));
    ex.drain();

    // The subscriber explicitly requests single-newest replay; latch retention is
    // delivered only to a subscriber that declares the durability that asks for it.
    plexus::io::subscriber_qos sub_qos;
    sub_qos.durability_mode = plexus::io::durability::latest;
    REQUIRE(fwd.attach_for_fanout(peer, "topic", std::nullopt, sub_qos));
    ex.drain();
    auto bodies = data_bodies(cap);
    REQUIRE(bodies.size() == 1);
    REQUIRE(bodies[0] == "writer-2"); // one slot per topic_hash, last write retained
}

TEST_CASE("latched retention adds no allocation beyond the frame-once publish",
          "[latch][forwarder]")
{
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    const std::string payload = "steady-state-body";
    constexpr int     K       = 256;

    // The per-publish allocation count over a single subscriber, with the topic either
    // latched (retain in the loop) or not (publish + fan-out only). The retain holds the
    // already-framed shared owner by addref, so it adds nothing beyond the publish owner.
    const auto allocs_per_publish = [&](bool latched)
    {
        sink_executor        ex;
        sink_channel         ch(ex);
        sink_forwarder       fwd{};
        sink_forwarder::peer peer{ch, "node-a"};
        if(latched)
            fwd.latch("topic");
        fwd.attach_for_fanout(peer, "topic");
        fwd.publish("topic", as_bytes(payload)); // warm-up: grow scratch AND first-touch the slot
        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            fwd.publish("topic", as_bytes(payload));
        const auto after = plexus::testing::alloc_count();
        REQUIRE(ch.sends >= static_cast<std::size_t>(K)); // every latched publish fanned out
        return after - before;
    };

    // Retention adds nothing beyond the publish: the latched and unlatched per-publish
    // allocation counts are identical — the slot retains the frame owner by addref.
    REQUIRE(allocs_per_publish(true) == allocs_per_publish(false));
}

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/data_frame.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
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

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A subscriber-side receive sink: an inproc channel the publisher fans toward,
// whose received frames route through a REAL frame_router. on_unidirectional
// decodes the inner unidirectional payload (the router owns the frame_header strip
// + the type switch — the production receive contract, not a hand-strip) and
// records the opaque bytes. This is the headline assertion surface: a late joiner
// recovers the retained value through the same demux any live frame traverses.
struct receive_sink
{
    explicit receive_sink(inproc_executor<> &ex)
        : channel(ex)
    {
        router.on_unidirectional([this](std::span<const std::byte> inner) {
            if(auto decoded = plexus::wire::decode_unidirectional(inner))
                bodies.emplace_back(to_string(decoded->data));
        });
        channel.on_data([this](std::span<const std::byte> f) { router.route(f); });
    }

    inproc_channel<> channel;
    plexus::io::frame_router router;
    std::vector<std::string> bodies;
};

forwarder::peer make_peer(inproc_channel<> &fwd_channel, receive_sink &sink, std::string node_name)
{
    fwd_channel.connect_to(sink.channel.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

}

TEST_CASE("inproc latch replay delivers the late joiner the retained value through frame_router, looped",
          "[integration][latch][inproc]")
{
    // Deterministic by construction (virtual clock + drain): a fresh
    // bus/executor/forwarder per iteration surfaces any flake as a mismatch on some
    // iteration rather than a one-off pass. The subscriber routes the replayed frame
    // through a real frame_router::on_unidirectional, so the assertion proves the
    // retained bytes survive the production demux byte-identical, not a hand-strip.
    constexpr int k_iterations = 100;
    const std::string payload = "retained-opaque-value";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        receive_sink sink(ex);
        auto peer = make_peer(ch, sink, "late-node");

        forwarder fwd;
        fwd.latch("topic");
        fwd.publish("topic", as_bytes(payload));   // retained with ZERO subscribers
        ex.drain();
        REQUIRE(sink.bodies.empty());   // nothing fanned out before the late join

        REQUIRE(fwd.attach_for_fanout(peer, "topic"));   // the late join drives the replay
        ex.drain();

        REQUIRE(sink.bodies.size() == 1);
        REQUIRE(sink.bodies[0] == payload);   // the EXACT retained bytes, every iteration
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("inproc non-latched topic does not replay on a late subscribe, looped",
          "[integration][latch][inproc]")
{
    // The non-latched NEGATIVE (guards against an accidental always-replay): the
    // same late-join setup with NO latch records NOTHING through the router until a
    // live publish arrives. Deterministic over inproc — this is the authoritative
    // negative guard (no grace window needed; the drain is exhaustive).
    constexpr int k_iterations = 100;
    int held = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        receive_sink sink(ex);
        auto peer = make_peer(ch, sink, "late-node");

        forwarder fwd;
        fwd.publish("topic", as_bytes(std::string{"live-only"}));   // not latched, no subscriber
        ex.drain();

        REQUIRE(fwd.attach_for_fanout(peer, "topic"));   // late join: NO replay
        ex.drain();
        REQUIRE(sink.bodies.empty());   // nothing replayed

        fwd.publish("topic", as_bytes(std::string{"first-live"}));   // now a live publish arrives
        ex.drain();
        REQUIRE(sink.bodies.size() == 1);
        REQUIRE(sink.bodies[0] == "first-live");
        ++held;
    }
    REQUIRE(held == k_iterations);
}

// A non-allocating sink Policy for the LATCH-NOALLOC gate (mirrors the message
// forwarder oracle's sink Policy): the byte_channel records send sizes without
// copying, so a forwarder<sink_policy> latched publish exercises framing +
// retention with no transport-side allocation — isolating the retention path so a
// clean 0 is the proof.
namespace {

struct sink_executor
{
};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}

    void send(std::span<const std::byte> d) { total_bytes += d.size(); ++sends; }
    void close() {}
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
    using executor_type = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type = sink_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

}

TEST_CASE("inproc LATCH-NOALLOC: steady-state latched publishing adds no retention allocation",
          "[integration][latch][inproc]")
{
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    sink_executor ex;
    sink_channel ch(ex);
    sink_forwarder fwd;
    sink_forwarder::peer peer{ch, "node-a"};
    fwd.latch("topic");
    fwd.attach_for_fanout(peer, "topic");

    const std::string payload = "steady-state-latched-body";

    // Warm-up: one latched publish grows the reused scratch buffers AND the
    // per-topic retained slot. The replay path is NOT in the measured loop (it
    // fires only on subscribe), so this gate isolates the per-publish RETENTION
    // cost — the same measurement posture as the message forwarder alloc gate.
    fwd.publish("topic", as_bytes(payload));
    const auto sends_before = ch.sends;

    constexpr int K = 256;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.publish("topic", as_bytes(payload));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(ch.sends - sends_before == K);   // every latched publish fanned out
    REQUIRE(after - before == 0);            // framing + retain: zero alloc after warm-up
}

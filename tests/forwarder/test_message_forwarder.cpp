#include "plexus/io/message_forwarder.h"
#include "plexus/io/wire_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/log/logger.h"
#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
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

// SLICE-4 maintainability gate: the forwarder models the wire_forwarder shape.
static_assert(plexus::io::wire_forwarder<forwarder, forwarder::peer>);

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A capture sink: a second inproc channel paired to the channel the forwarder
// sends over, recording every delivered frame so the test can decode what the
// forwarder emitted (subscribe / unsubscribe / data) after a drain().
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

// Wires fwd_channel -> cap.sink so a forwarder send() on fwd_channel surfaces in
// cap.frames after drain(). Returns a peer keyed on node_name.
forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Counts frame_header-wrapped subscribe_request frames in a capture's recorded
// traffic. Control frames are now framed (Seed 3), so the helper FIRST strips and
// asserts the frame_header.type, THEN decodes the inner payload — a frame whose
// header type is not `subscribe` is not counted.
std::size_t count_subscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_subscribe_request(inner))
            ++n;
    }
    return n;
}

std::size_t count_unsubscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::unsubscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_unsubscribe_request(inner))
            ++n;
    }
    return n;
}

// A test logger whose warn() bumps a counter — proves the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view) override { ++count; }
    std::size_t count{0};
};

// A non-allocating sink Policy used solely by the forwarder-level alloc gate.
// Its byte_channel records each send's size into a counter without copying the
// bytes, so a forwarder<sink_policy> publish exercises framing + the fan-out
// dispatch loop with no transport-side allocation — isolating the forwarder's
// own heap behavior (the inproc bus's per-packet copy is the transport's, and is
// audited separately). The timer/executor are inert: the alloc gate never steps.
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

// Builds a valid framed unidirectional message for the named fqn carrying body.
std::vector<std::byte> make_data_frame(std::string_view fqn, const std::string &body)
{
    plexus::wire::unidirectional_header uhdr{
            .source     = plexus::wire::endpoint_source_type::publisher,
            .sequence   = 0,
            .topic_hash = plexus::wire::fqn_topic_hash(fqn)
    };
    auto inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));
    plexus::wire::frame_header fhdr{
            .type         = plexus::wire::msg_type::unidirectional,
            .flags        = 0,
            .session_id   = 0,
            .timestamp_ns = 0,
            .payload_len  = inner.size()
    };
    return plexus::wire::encode_frame(fhdr, inner);
}

}

TEST_CASE("attach refcount gate emits exactly one subscribe on 0->1", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd;
        REQUIRE(fwd.attach(peer, "alpha"));    // 0->1
        REQUIRE_FALSE(fwd.attach(peer, "alpha")); // 1->2, no emit
        ex.drain();

        REQUIRE(count_subscribes(cap) == 1);
    }
}

TEST_CASE("drain_for re-emits one subscribe per peer-rooted remote topic", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch(ex);
        capture cap(ex);
        auto peer = make_peer(ch, cap, "node-a");

        forwarder fwd;
        fwd.attach(peer, "alpha");
        fwd.attach(peer, "beta");
        ex.drain();
        cap.frames.clear();

        fwd.drain_for(peer);
        ex.drain();
        REQUIRE(count_subscribes(cap) == 2);   // one per recorded remote topic
    }
}

TEST_CASE("frame-once fan-out delivers byte-identical frames to each subscriber", "[forwarder]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        inproc_bus<> bus;
        inproc_executor<> ex(bus);
        inproc_channel<> ch_a(ex), ch_b(ex);
        capture cap_a(ex), cap_b(ex);
        auto peer_a = make_peer(ch_a, cap_a, "node-a");
        auto peer_b = make_peer(ch_b, cap_b, "node-b");

        forwarder fwd;
        fwd.attach(peer_a, "alpha");
        fwd.attach(peer_b, "alpha");
        ex.drain();
        cap_a.frames.clear();
        cap_b.frames.clear();

        fwd.publish("alpha", as_bytes(std::string{"payload-bytes"}));
        ex.drain();

        REQUIRE(cap_a.frames.size() == 1);
        REQUIRE(cap_b.frames.size() == 1);
        // Byte-identical, not merely equal-length: no per-peer session stamp.
        REQUIRE(cap_a.frames[0] == cap_b.frames[0]);
    }
}

TEST_CASE("detach_all stops fan-out", "[forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    fwd.attach(peer, "alpha");
    ex.drain();
    cap.frames.clear();

    fwd.detach_all(peer);
    fwd.publish("alpha", as_bytes(std::string{"after-detach"}));
    ex.drain();
    REQUIRE(cap.frames.empty());   // no subscriber remains
}

TEST_CASE("detach on 1->0 emits exactly one unsubscribe_request", "[forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    fwd.attach(peer, "alpha");
    fwd.attach(peer, "alpha");      // refcount 2
    ex.drain();
    cap.frames.clear();

    REQUIRE_FALSE(fwd.detach(peer, "alpha")); // 2->1, no emit
    REQUIRE(fwd.detach(peer, "alpha"));        // 1->0, emit
    ex.drain();
    REQUIRE(count_unsubscribes(cap) == 1);
}

TEST_CASE("receive tail resolves the fqn by topic_hash and hands exact bytes up", "[forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    fwd.attach(peer, "alpha");     // registers the topic_hash -> fqn resolution

    const std::string body = "the-opaque-payload";
    auto frame = make_data_frame("alpha", body);

    // The frame_router owns the header strip + type switch; its unidirectional
    // consumer hands the inner payload (header-off) to the realigned deliver().
    std::string got_fqn;
    std::string got_body;
    plexus::io::frame_router router;
    router.on_unidirectional([&](const plexus::wire::frame_header &, std::span<const std::byte> inner) {
        fwd.deliver(peer, inner, [&](std::string_view fqn, std::span<const std::byte> data) {
            got_fqn.assign(fqn);
            got_body.assign(reinterpret_cast<const char *>(data.data()), data.size());
        });
    });
    router.route(frame);

    REQUIRE(got_fqn == "alpha");
    REQUIRE(got_body == body);
}

TEST_CASE("no-subscriber publish sends nothing (demand-driven)", "[forwarder]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);

    forwarder fwd;
    fwd.publish("alpha", as_bytes(std::string{"nobody-home"}));
    ex.drain();
    REQUIRE_FALSE(bus.has_pending_packets());   // nothing was ever enqueued
}

TEST_CASE("receive tail warn-and-drops a malformed frame through the injected logger", "[forwarder]")
{
    counting_logger log;
    forwarder fwd(log);

    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    // An inner payload too short to be a unidirectional header (< 25 bytes), so
    // the realigned deliver() — which now receives the header-off inner payload
    // (the router owns the header strip) — fails decode_unidirectional and drops.
    std::vector<std::byte> garbage(8, std::byte{0xAB});

    bool fired = false;
    fwd.deliver(peer, garbage, [&](std::string_view, std::span<const std::byte>) { fired = true; });

    REQUIRE_FALSE(fired);           // dropped: no subscriber callback
    REQUIRE(log.count == 1);        // the warn seam fired exactly once
}

TEST_CASE("default forwarder drops a malformed frame silently via null_logger", "[forwarder]")
{
    forwarder fwd;                  // no logger argument: shared null_logger

    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    std::vector<std::byte> garbage(8, std::byte{0xAB});

    bool fired = false;
    REQUIRE_NOTHROW(fwd.deliver(peer, garbage, [&](std::string_view, std::span<const std::byte>) {
        fired = true;
    }));
    REQUIRE_FALSE(fired);           // dropped silently, no crash
}

TEST_CASE("frame-once fan-out allocates nothing after warm-up", "[forwarder]")
{
    // Measured over the non-allocating sink Policy so the only heap traffic in
    // the loop would be the forwarder's own: framing into the reused scratch +
    // the fan-out dispatch. The inproc bus's per-packet copy is the transport's
    // allocation and is audited separately (plan 06).
    using sink_forwarder = plexus::io::message_forwarder<sink_policy>;

    sink_executor ex;
    sink_channel ch_a(ex), ch_b(ex);
    sink_forwarder fwd;
    sink_forwarder::peer peer_a{ch_a, "node-a"};
    sink_forwarder::peer peer_b{ch_b, "node-b"};
    fwd.attach(peer_a, "alpha");
    fwd.attach(peer_b, "alpha");

    const std::string payload = "steady-state-body";

    // Warm-up: one publish grows the scratch buffers to their steady-state size.
    fwd.publish("alpha", as_bytes(payload));
    const auto sends_a_before = ch_a.sends;
    const auto sends_b_before = ch_b.sends;

    constexpr int K = 256;
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.publish("alpha", as_bytes(payload));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(ch_a.sends - sends_a_before == K);  // every publish fanned to both
    REQUIRE(ch_b.sends - sends_b_before == K);
    REQUIRE(after - before == 0);   // forwarder framing + dispatch: zero alloc
}

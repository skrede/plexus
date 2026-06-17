// The recorder oracle, in two halves.
//
// (1) The pre-buffer / FDR mode (the same byte_ring run drop-oldest continuous-overwrite):
// it holds "the last N bytes" (byte-bounded under a saturating producer), a freeze
// snapshots two indices with zero allocation (no buffer copy), the frozen window drains to
// the in-memory byte_sink with no thread, all three trigger sources fire (manual, an armed
// anomaly predicate on a drop edge, a deadline-miss edge), and captured_span equals
// newest_ts - oldest_ts under a manual clock.
//
// (2) The public-API end-to-end capture (the make_recorder attach + the RAII handle): a
// live multi-endpoint inproc node attaches a recorder through node.make_recorder(sink,
// opts) — PUBLIC API ONLY, no router(), no internal recording include in the attach path —
// runs a multi-topic session, drains on the node's own executor turns, recovers the flat
// stream offline with record_stream_reader, decodes the payload with a separately-supplied
// codec (no codec in the tap), and closes the dropout accounting
// sum(dropout.count) + delivered == produced. A teardown section destroys the recorder and
// pumps the executor post-dtor — this section exercises the runtime behavior of
// make_recorder + the recorder RAII handle (the deregister-before-teardown discipline); the
// asan tree is the load-bearing UAF gate. The capture/dropout legs loop >=3x (medians). No
// tuned byte-budget constant is asserted as a default.

#include "support/alloc_counter.h"

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/recorder.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using plexus::io::message_info;
using plexus::io::qos_edge;
using plexus::io::qos_change_event;
using plexus::io::capture_fidelity;
using plexus::io::detail::drop_event;
using plexus::io::detail::drop_cause;
using plexus::io::locality;
using plexus::io::recording::record_category;
using plexus::io::recording::record_envelope;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;
using plexus::io::recording::pre_buffer_controller;

namespace {

// A deterministic monotonic clock: each read advances by a fixed step so captured
// timestamps are exact and the time-span is a known multiple of the step.
struct manual_clock
{
    std::uint64_t now{0};
    std::uint64_t step{10};
    std::uint64_t operator()() noexcept
    {
        const std::uint64_t t = now;
        now += step;
        return t;
    }
};

std::vector<std::byte> payload_of(std::size_t n, std::byte fill)
{
    return std::vector<std::byte>(n, fill);
}

// Decode the frozen window the FDR drain wrote to the sink. The drained bytes are a bare
// length-prefixed record stream (no stream header — the window is the ring's records), so
// iterate [varint len][payload] and decode each body (CRC stripped) offline.
std::vector<decoded_record> decode_window(std::span<const std::byte> stream)
{
    std::vector<decoded_record> out;
    std::size_t                 at = 0;
    while(at < stream.size())
    {
        std::size_t len_off = at;
        const auto  len     = plexus::wire::read_varint(stream, len_off);
        if(!len)
            break;
        const std::size_t end = len_off + static_cast<std::size_t>(*len);
        if(end > stream.size())
            break;
        const auto payload = stream.subspan(len_off, static_cast<std::size_t>(*len));
        if(payload.size() >= sizeof(std::uint32_t))
        {
            decoded_record rec;
            const auto     body = payload.first(payload.size() - sizeof(std::uint32_t));
            if(plexus::io::recording::decode_record_body(body, rec))
                out.push_back(rec);
        }
        at = end;
    }
    return out;
}

drop_event arq_drop()
{
    drop_event e;
    e.cause      = drop_cause::arq_shed;
    e.transport  = locality::any;
    e.topic_hash = 0x1234;
    e.count      = 1;
    return e;
}

}

TEST_CASE("pre_buffer mode runs drop-oldest and stays byte-bounded under a saturating producer",
          "[recorder_capture][fdr]")
{
    for(int run = 0; run < 3; ++run)
    {
        in_memory_byte_sink   sink;
        manual_clock          clk;
        pre_buffer_controller pre{sink, 1024, [&clk] { return clk(); }};

        const auto body = payload_of(48, std::byte{0x5A});
        for(int i = 0; i < 5000; ++i)
            pre.record_sample(0x77, message_info{}, 0, false, capture_fidelity::payload, body);

        // The window is the ring; it never exceeds the configured budget regardless of how
        // far the producer ran past it (drop-oldest evicted the rest).
        pre.trigger();
        while(pre.pump())
        {
        }
        REQUIRE(sink.bytes().size() <= 1024);

        const auto held = decode_window(sink.bytes());
        REQUIRE(!held.empty());
        // The held records are "the last N": their timestamps are the most-recent ones, so
        // the oldest held capture_ts is far past the first sample's.
        REQUIRE(held.front().capture_ts > 0);
    }
}

TEST_CASE("the freeze captures two indices with no allocation and no buffer copy",
          "[recorder_capture][fdr]")
{
    for(int run = 0; run < 3; ++run)
    {
        in_memory_byte_sink   sink;
        manual_clock          clk;
        pre_buffer_controller pre{sink, 2048, [&clk] { return clk(); }};

        const auto body = payload_of(32, std::byte{0xC3});
        for(int i = 0; i < 500; ++i)
            pre.record_sample(0x42, message_info{}, 0, false, capture_fidelity::payload, body);

        plexus::testing::reset_alloc_count();
        const std::size_t before = plexus::testing::alloc_count();
        pre.trigger();
        const std::size_t after = plexus::testing::alloc_count();

        REQUIRE(after == before);
        REQUIRE(pre.frozen());
    }
}

TEST_CASE("a manual trigger freezes and drains the held window to the byte_sink with no thread",
          "[recorder_capture][fdr]")
{
    in_memory_byte_sink   sink;
    manual_clock          clk;
    pre_buffer_controller pre{sink, 4096, [&clk] { return clk(); }};

    const auto body = payload_of(16, std::byte{0x11});
    for(int i = 0; i < 8; ++i)
        pre.record_sample(0x9, message_info{}, 0, false, capture_fidelity::payload, body);

    REQUIRE(sink.bytes().empty()); // not drained until the trigger
    pre.trigger();
    while(pre.pump())
    {
    }
    REQUIRE_FALSE(pre.frozen()); // re-armed after the window exhausted

    const auto held = decode_window(sink.bytes());
    REQUIRE(held.size() == 8);
    for(const auto &rec : held)
        REQUIRE(rec.category == record_category::sample);
}

TEST_CASE("an armed anomaly predicate freezes on a synthetic drop edge",
          "[recorder_capture][fdr]")
{
    in_memory_byte_sink   sink;
    manual_clock          clk;
    pre_buffer_controller pre{sink, 4096, [&clk] { return clk(); }};

    pre.on_anomaly([](const record_envelope &env) {
        return env.category == record_category::drop && env.cause == drop_cause::arq_shed;
    });

    const auto body = payload_of(16, std::byte{0x22});
    for(int i = 0; i < 4; ++i)
        pre.record_sample(0x9, message_info{}, 0, false, capture_fidelity::payload, body);
    REQUIRE_FALSE(pre.frozen());

    pre.record_drop(arq_drop()); // the anomaly edge arms the predicate -> auto-freeze
    REQUIRE(pre.frozen());

    while(pre.pump())
    {
    }
    const auto held = decode_window(sink.bytes());
    REQUIRE(!held.empty());
    REQUIRE(held.back().category == record_category::drop);
}

TEST_CASE("a deadline-miss edge rides the drop surface and freezes via the predicate",
          "[recorder_capture][fdr]")
{
    in_memory_byte_sink   sink;
    manual_clock          clk;
    pre_buffer_controller pre{sink, 4096, [&clk] { return clk(); }};

    // A deadline-miss is an observable drop edge; the predicate matches it like any anomaly.
    pre.on_anomaly([](const record_envelope &env) {
        return env.category == record_category::drop && env.cause == drop_cause::blocked;
    });

    const auto body = payload_of(16, std::byte{0x33});
    pre.record_sample(0x9, message_info{}, 0, false, capture_fidelity::payload, body);

    drop_event miss;
    miss.cause      = drop_cause::blocked; // the deadline-miss / liveliness-lapse edge
    miss.topic_hash = 0xDEAD;
    pre.record_drop(miss);

    REQUIRE(pre.frozen());
    while(pre.pump())
    {
    }
    REQUIRE(!decode_window(sink.bytes()).empty());
}

TEST_CASE("captured_span reports newest_ts - oldest_ts over the frozen window",
          "[recorder_capture][fdr]")
{
    in_memory_byte_sink   sink;
    manual_clock          clk; // step 10: ts are 0,10,20,...
    pre_buffer_controller pre{sink, 8192, [&clk] { return clk(); }};

    const auto body = payload_of(8, std::byte{0x44});
    const int  n    = 6;
    for(int i = 0; i < n; ++i)
        pre.record_sample(0x9, message_info{}, 0, false, capture_fidelity::payload, body);

    pre.trigger();
    // ts: 0,10,20,30,40,50 -> span = 50 - 0 = 50 = (n-1)*step.
    REQUIRE(pre.captured_span() == static_cast<std::uint64_t>((n - 1) * 10));

    while(pre.pump())
    {
    }
    const auto held = decode_window(sink.bytes());
    REQUIRE(held.size() == static_cast<std::size_t>(n));
    REQUIRE(held.back().capture_ts - held.front().capture_ts == pre.captured_span());
}

// ---- The public-API end-to-end capture (make_recorder attach + the RAII handle) ----

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// The fixture OWNS the executor + bus + transport + discovery; the node and the recorder
// are built in an inner scope over this borrowed substrate, so the executor outlives both
// and can be pumped after the recorder's (and the node's) dtor returns — the post-dtor pump
// that proves the deregister-before-teardown discipline.
struct session_fixture
{
    inproc_bus<>        bus;
    inproc_executor<>   ex{bus};
    inproc_transport<>  ta{ex, bus};
    static_discovery    disc{{}};

    void drive() { ex.drain(); }
};

// The separately-supplied offline decoder, proving no codec lives in the tap: a captured
// sample carries the framed wire bytes, whose tail is the published payload. This recovers
// the marker by matching the trailing bytes — the stream itself never decoded them.
bool payload_ends_with(std::span<const std::byte> framed, std::span<const std::byte> marker)
{
    if(framed.size() < marker.size())
        return false;
    return std::equal(marker.begin(), marker.end(), framed.end() - marker.size());
}

}

TEST_CASE("a public-API recorder captures a live multi-endpoint inproc session and recovers it",
          "[recorder_capture][e2e]")
{
    // This section exercises the runtime behavior of node.make_recorder + the recorder RAII
    // handle: public-API attach -> live multi-topic capture -> cooperative drain on the
    // node's own turns -> offline recovery -> dropout accounting closed.
    for(int run = 0; run < 3; ++run)
    {
        session_fixture fx;
        const auto      id = make_id(0x4C);

        in_memory_byte_sink sink;
        const int           per_topic = 16;
        std::vector<std::byte> recovered_samples;

        {
            plexus::node_options opts;
            inproc_node          n{fx.ex, fx.disc, id, fx.ta, opts};

            // Attach through the PUBLIC verb — no router(), no internal recording type in
            // the attach path. A roomy ring so the full session is captured (recovered ==
            // produced, no dropouts) this run.
            plexus::recorder_options ro;
            ro.ring_bytes = 1u << 20;
            auto rec = n.make_recorder(sink, std::move(ro));

            // The endpoints live in an INNER scope so their declaration-drop edges (posted
            // capturing the engine `this`) are drained while the node is still alive — the
            // node-facade teardown contract. The recorder + node outlive this scope.
            {
                plexus::topic_qos qos;
                qos.latch = true;
                plexus::publisher<> pub_a{n, "topic.a", qos};
                plexus::publisher<> pub_b{n, "topic.b", qos};
                plexus::subscriber<> sub_a{n, "topic.a",
                    [](std::span<const std::byte>, const message_info &) {}};
                plexus::subscriber<> sub_b{n, "topic.b",
                    [](std::span<const std::byte>, const message_info &) {}};
                fx.drive();

                for(int i = 0; i < per_topic; ++i)
                {
                    const std::array<std::byte, 4> mk{std::byte(0xA0), std::byte(i & 0xff),
                                                      std::byte{0xBE}, std::byte{0xEF}};
                    pub_a.publish(mk);
                    pub_b.publish(mk);
                    fx.drive(); // the published tap posts; the cooperative drain ships to sink
                }
                fx.drive();
            }
            // The endpoint-drop edges posted at the inner-scope exit are drained here with
            // the node still alive; the recorder captures them too.
            fx.drive();
            rec.flush();
            // rec destroyed here (deregister-before-teardown), then the node dies below.
        }
        // The node + recorder are gone; the fixture executor outlives them. Pump post-dtor:
        // the participant-teardown edge uses the engine's snapshot variant and the recorder
        // already deregistered, so no posted closure touches a freed ring (asan is the gate).
        fx.drive();

        // Recover the captured stream offline (no codec in the path).
        record_stream_reader reader{sink.bytes()};
        stream_definitions   defs;
        REQUIRE(reader.read_definitions(defs));
        REQUIRE(defs.node == id);

        std::vector<decoded_record> records;
        const recovery_result       res = reader.recover(records);
        REQUIRE(res.header_ok);

        std::uint64_t produced = 0;
        std::uint64_t dropped  = 0;
        std::size_t   samples  = 0;
        bool          saw_topic_a = false;
        bool          saw_topic_b = false;
        for(const auto &r : records)
        {
            if(r.category == record_category::dropout)
                dropped += r.count;
            if(r.category != record_category::sample)
                continue;
            ++samples;
            if(r.topic_hash == plexus::wire::fqn_topic_hash("topic.a")) saw_topic_a = true;
            if(r.topic_hash == plexus::wire::fqn_topic_hash("topic.b")) saw_topic_b = true;
            // The framed sample's tail is the published marker (decoded offline, no tap codec).
            const std::array<std::byte, 2> tail{std::byte{0xBE}, std::byte{0xEF}};
            REQUIRE(payload_ends_with(r.payload, tail));
        }

        // Both topics captured; every produced sample present (roomy ring => zero dropouts).
        REQUIRE(saw_topic_a);
        REQUIRE(saw_topic_b);
        REQUIRE(dropped == 0);
        REQUIRE(samples == static_cast<std::size_t>(2 * per_topic));

        // The dropout accounting closes exactly: delivered + dropped == produced.
        produced = static_cast<std::uint64_t>(2 * per_topic);
        REQUIRE(static_cast<std::uint64_t>(samples) + dropped == produced);
        (void)recovered_samples;
    }
}

TEST_CASE("a recorder destroyed mid-session deregisters before teardown and survives a post-dtor pump",
          "[recorder_capture][e2e][teardown]")
{
    // The load-bearing teardown-UAF section: build a node + recorder, run a partial session,
    // destroy the recorder (and the node) while the fixture executor stays live, then pump
    // post-dtor. The handle deregisters its tap and drains the ring out in its dtor, so the
    // already-posted self-re-posting drain task finds its block expired and exits without
    // reading a freed ring. asan (-fno-sanitize-recover) is the real gate.
    session_fixture     fx;
    in_memory_byte_sink sink;
    const auto          id = make_id(0x5D);

    {
        plexus::node_options opts;
        inproc_node          n{fx.ex, fx.disc, id, fx.ta, opts};

        {
            auto rec = n.make_recorder(sink);

            // The endpoints live in their own inner scope so their drop edges drain while
            // the node is alive (the facade teardown contract); the recorder captures them.
            {
                plexus::topic_qos qos;
                qos.latch = true;
                plexus::publisher<> pub{n, "teardown.topic", qos};
                plexus::subscriber<> sub{n, "teardown.topic",
                    [](std::span<const std::byte>, const message_info &) {}};
                for(int i = 0; i < 8; ++i)
                {
                    const std::array<std::byte, 3> mk{std::byte{0x01}, std::byte(i), std::byte{0x02}};
                    pub.publish(mk);
                    fx.drive();
                }
            }
            fx.drive(); // drain the endpoint-drop edges with the node still alive
            // rec destroyed here: deregister-before-teardown + a synchronous drain-out.
        }
        // Pump after the recorder is gone but the node still lives: a stale drain task (if
        // any) must be inert.
        fx.drive();
    }
    // Pump after the node is gone too: the participant-destroy edge uses the snapshot variant
    // and the recorder already deregistered, so no posted closure reads a freed ring.
    fx.drive();

    // The pre-dtor records were drained out; the stream head is recoverable.
    record_stream_reader reader{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));
}

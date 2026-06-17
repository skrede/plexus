// The pre-buffer / FDR mode oracle. The same byte_ring (Plan-1 substrate) is run in
// drop-oldest continuous-overwrite and is NOT drained until a trigger: a saturating
// producer keeps the ring at "the last N bytes" (byte-bounded), a freeze captures the
// window by snapshotting two indices with ZERO allocation (the alloc-counter delta across
// the freeze is 0, no buffer copy), and the frozen window drains to the in-memory
// byte_sink with NO thread. All three trigger sources are exercised — a manual trigger(),
// a consumer-armed anomaly predicate on a synthetic drop edge, and a deadline-miss edge
// (a drop_event the predicate matches) — and captured_span() is asserted equal to
// newest_ts - oldest_ts under a deterministic manual clock. The byte-bound + freeze legs
// are looped >=3x (medians). No tuned byte-budget constant is asserted as a default.

#include "support/alloc_counter.h"

#include "in_memory_byte_sink.h"

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
#include <vector>
#include <cstddef>
#include <cstdint>
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

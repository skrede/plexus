#include "test_recorder_capture_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recorder_capture_fixture;

TEST_CASE("an armed anomaly predicate freezes on a synthetic drop edge", "[recorder_capture][fdr]")
{
    in_memory_byte_sink sink;
    manual_clock clk;
    pre_buffer_controller pre{sink, 4096, [&clk] { return clk(); }};

    pre.on_anomaly([](const record_envelope &env) { return env.category == record_category::drop && env.cause == drop_cause::arq_shed; });

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

TEST_CASE("a deadline-miss edge rides the drop surface and freezes via the predicate", "[recorder_capture][fdr]")
{
    in_memory_byte_sink sink;
    manual_clock clk;
    pre_buffer_controller pre{sink, 4096, [&clk] { return clk(); }};

    // A deadline-miss is an observable drop edge; the predicate matches it like any anomaly.
    pre.on_anomaly([](const record_envelope &env) { return env.category == record_category::drop && env.cause == drop_cause::blocked; });

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

TEST_CASE("captured_span reports newest_ts - oldest_ts over the frozen window", "[recorder_capture][fdr]")
{
    in_memory_byte_sink sink;
    manual_clock clk; // step 10: ts are 0,10,20,...
    pre_buffer_controller pre{sink, 8192, [&clk] { return clk(); }};

    const auto body = payload_of(8, std::byte{0x44});
    const int n     = 6;
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

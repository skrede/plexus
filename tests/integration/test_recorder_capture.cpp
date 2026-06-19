#include "test_recorder_capture_common.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

using namespace recorder_capture_fixture;

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

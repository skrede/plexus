#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_WORKLOAD_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_WORKLOAD_H

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/message_info.h"

#include "esp_timer.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <span>
#include <array>
#include <cstdio>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace example {

// The fixed payload tiers, bounded by lwip_channel_limits::max_message_bytes (8 KiB). The large
// tier stays under the 5760 B advertised window so one window carries it — the bench measures the
// MCU-appropriate config, not a deep-window firehose.
inline constexpr std::array<std::size_t, 3> k_payload_tiers{16, 256, 4096};
inline constexpr std::size_t k_max_tier_bytes = 4096;

// Per (tier x policy) cell: a warm-up the aggregation discards, then the sampled requests. One
// in-flight request at a time keeps the round trip a clean device->host->device measurement.
inline constexpr std::uint32_t k_warmup_requests  = 8;
inline constexpr std::uint32_t k_sampled_requests = 128;

// One request/echo round trip in flight at a time: publish a tier-sized request stamped with the
// send time, await its echo on the reply topic, record the elapsed microseconds, then issue the
// next. The first byte carries a sequence tag so a stale echo is ignored, not mistimed.
struct echo_probe
{
    plexus::publisher<void> &request;
    std::size_t              tier;
    std::array<std::byte, k_max_tier_bytes> buffer{};
    std::int64_t             sent_us{0};
    std::uint8_t             seq{0};
    bool                     awaiting{false};

    void issue()
    {
        buffer[0] = static_cast<std::byte>(++seq);
        sent_us   = esp_timer_get_time();
        awaiting  = true;
        request.publish(std::span<const std::byte>{buffer.data(), tier});
    }

    bool matches(std::span<const std::byte> echo) const
    {
        return awaiting && !echo.empty() && echo[0] == static_cast<std::byte>(seq);
    }
};

// Emit one machine-parseable sample line on the console (UART0) the runner parses. Fixed fields,
// space-separated key=value pairs under a stable BENCH tag — no localized formatting.
inline void emit_sample(const char *policy, std::size_t tier, std::uint32_t index, std::int64_t rtt_us)
{
    std::printf("BENCH sample policy=%s tier=%u index=%u rtt_us=%lld\n", policy, static_cast<unsigned>(tier), static_cast<unsigned>(index), static_cast<long long>(rtt_us));
}

// Emit the RX-task RAM/stack cost line once per policy: the RX task's stack high-water (words, the
// FreeRTOS unit) and the free-heap reading. P1 reports rx_stack=0 (no RX task); the runner takes
// the P2-minus-P1 free-heap delta as the RX-task incremental cost.
inline void emit_resource(const char *policy, std::uint32_t rx_stack_words, std::uint32_t free_heap)
{
    std::printf("BENCH resource policy=%s rx_stack_words=%u free_heap=%u\n", policy, static_cast<unsigned>(rx_stack_words), static_cast<unsigned>(free_heap));
}

}

#endif

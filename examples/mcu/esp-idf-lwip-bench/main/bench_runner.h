#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_RUNNER_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_RUNNER_H

#include "bench_workload.h"

#include "plexus/freertos/freertos_timer.h"

#include "esp_heap_caps.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace example {

// The cell sweep: it walks the tiers, issuing requests on each and recording round trips, then
// advances to the next tier; once all tiers drain it emits the RX-task resource line and stops. The
// reply callback drives the advance — one request in flight at a time, the next issued on receipt.
struct bench_runner
{
    const char  *policy;
    echo_probe  &probe;
    std::size_t  tier_index{0};
    std::uint32_t count{0};

    void start()
    {
        probe.tier = k_payload_tiers[tier_index];
        probe.issue();
    }

    void on_reply(std::span<const std::byte> echo)
    {
        if(!probe.matches(echo))
            return;
        const std::int64_t rtt = esp_timer_get_time() - probe.sent_us;
        record(rtt);
        advance();
    }

    void record(std::int64_t rtt_us)
    {
        if(count >= k_warmup_requests)
            emit_sample(policy, probe.tier, count - k_warmup_requests, rtt_us);
        ++count;
    }

    void advance()
    {
        if(count < k_warmup_requests + k_sampled_requests)
            return probe.issue();
        count = 0;
        if(++tier_index < k_payload_tiers.size())
            return start();
        probe.awaiting = false;
        report_resource();
    }

    void report_resource()
    {
        const TaskHandle_t  rx      = xTaskGetHandle("plexus-rx");
        const std::uint32_t rx_high = rx ? uxTaskGetStackHighWaterMark(rx) : 0;
        emit_resource(policy, rx_high, esp_get_free_heap_size());
    }
};

// The first request races the async dial/handshake/subscription-propagation and the single-in-flight
// chain has no resend; re-issue a request stalled past a threshold above the SLOWEST real round trip.
// The binding case is the serial cell's 4096 B tier: each 4096 B leg is ~356 ms at 115200 8N1, so a
// round trip is ~0.8 s. Below that, issue() (which bumps the seq) re-fires before the real echo
// returns, and on_reply then rejects the echo as stale (matches() wants the current seq) — wedging
// that tier forever. The threshold sits well above it; the 250 ms timer is only the poll granularity.
struct resend_pump
{
    plexus::freertos::freertos_timer &timer;
    echo_probe                       &probe;
    static constexpr std::int64_t     k_stall_us = 2000000;

    void arm()
    {
        timer.expires_after(std::chrono::milliseconds{250});
        timer.async_wait([this](std::error_code ec) { on_tick(ec); });
    }

    void on_tick(std::error_code ec)
    {
        if(ec)
            return;
        if(probe.awaiting && esp_timer_get_time() - probe.sent_us > k_stall_us)
            probe.issue();
        arm();
    }
};

}

#endif

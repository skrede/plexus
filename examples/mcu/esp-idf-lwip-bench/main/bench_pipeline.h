#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_PIPELINE_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_PIPELINE_H

#include "bench_workload.h"

#include "plexus/publisher.h"

#include "plexus/freertos/freertos_timer.h"

#include "esp_timer.h"
#include "esp_heap_caps.h"

#include <span>
#include <array>
#include <cstdio>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace example {

// The pipelined request/echo probe: it holds K round trips in flight (a ring of K tagged slots) under
// congestion=block. At K=1 it reduces to the single-in-flight latency probe (one slot, refill-on-reply).
inline constexpr std::size_t k_max_window = 16;
// A slot stalled past this is re-issued so a lost reply (or the startup race) cannot wedge the window.
inline constexpr std::int64_t k_pipeline_stall_us = 2000000;
// A tier that cannot complete its sampled window in this budget is stall-limited: the small fixed RX slot
// pool cannot hold a deep window of the largest payload. Report the gross completions over the tier's wall
// clock (a buffer-bound tier is a measured result) and advance rather than wedging the sweep.
inline constexpr std::int64_t k_tier_deadline_us = 20000000;

struct slot
{
    bool         in_use;
    std::uint8_t tag;
    std::int64_t issued_us;
};

// Emit the pipeline throughput line; the per-completion samples reuse emit_sample so p50/p99
// latency-at-depth aggregates through the existing parser. K rides the policy label (e.g. lwip-p2-k4).
inline void emit_pipeline(const char *policy, std::size_t tier, std::size_t window, std::uint32_t completed, std::int64_t elapsed_us)
{
    std::printf("BENCH throughput policy=%s tier=%u window=%u completed=%u elapsed_us=%lld\n",
                policy, static_cast<unsigned>(tier), static_cast<unsigned>(window), static_cast<unsigned>(completed), static_cast<long long>(elapsed_us));
}

struct windowed_probe
{
    const char              *policy;
    plexus::publisher<void> &request;
    std::size_t              window;
    std::size_t              tier;
    std::array<std::byte, k_max_tier_bytes> buffer{};
    std::array<slot, k_max_window>          slots{};
    std::uint8_t             next_tag{0};
    std::size_t              tier_index{0};
    std::uint32_t            count{0};
    std::int64_t             sample_start{0};
    std::int64_t             tier_start_us{0};
    bool                     draining{false};
    bool                     running{true};

    void issue_slot(std::size_t i)
    {
        slots[i] = slot{true, next_tag, esp_timer_get_time()};
        buffer[0] = static_cast<std::byte>(next_tag);
        ++next_tag;
        request.publish(std::span<const std::byte>{buffer.data(), tier});
    }

    void issue_one()
    {
        for(std::size_t i = 0; i < window; ++i)
            if(!slots[i].in_use)
                return issue_slot(i);
    }

    void start()
    {
        tier          = k_payload_tiers[tier_index];
        tier_start_us = esp_timer_get_time();
        for(std::size_t i = 0; i < window; ++i)
            issue_slot(i);
    }

    bool all_free() const
    {
        for(std::size_t i = 0; i < window; ++i)
            if(slots[i].in_use)
                return false;
        return true;
    }

    void on_reply(std::span<const std::byte> echo)
    {
        if(!running || echo.empty())
            return;
        const std::uint8_t tag = static_cast<std::uint8_t>(echo[0]);
        for(std::size_t i = 0; i < window; ++i)
        {
            if(!slots[i].in_use || slots[i].tag != tag)
                continue;
            const std::int64_t rtt = esp_timer_get_time() - slots[i].issued_us;
            slots[i].in_use = false;
            complete(rtt);
            return;
        }
    }

    // Run-loop pace timer: enforce the per-tier deadline, then re-issue a genuinely lost slot so it never wedges.
    void pump_stalled()
    {
        if(!running)
            return;
        const std::int64_t now = esp_timer_get_time();
        if(!draining && now - tier_start_us > k_tier_deadline_us)
            return finalize_tier_partial(now);
        for(std::size_t i = 0; i < window; ++i)
            if(slots[i].in_use && now - slots[i].issued_us > k_pipeline_stall_us)
            {
                if(draining)
                    slots[i].in_use = false;
                else
                    issue_slot(i);
            }
        if(draining && all_free())
            next_tier();
    }

    void complete(std::int64_t rtt_us)
    {
        if(draining)
        {
            if(all_free())
                next_tier();
            return;
        }
        if(count == k_warmup_requests)
            sample_start = esp_timer_get_time();
        if(count >= k_warmup_requests)
            emit_sample(policy, tier, count - k_warmup_requests, rtt_us);
        ++count;
        if(count == k_warmup_requests + k_sampled_requests)
        {
            emit_pipeline(policy, tier, window, k_sampled_requests, esp_timer_get_time() - sample_start);
            draining = true;
            if(all_free())
                next_tier();
            return;
        }
        issue_one();
    }

    // Deadline hit: report the gross completions (completed < k_sampled_requests marks it stall-limited) and advance.
    void finalize_tier_partial(std::int64_t now)
    {
        emit_pipeline(policy, tier, window, count, now - tier_start_us);
        next_tier();
    }

    void next_tier()
    {
        if(++tier_index >= k_payload_tiers.size())
        {
            running = false;
            emit_resource(policy, 0, esp_get_free_heap_size());
            return;
        }
        count    = 0;
        draining = false;
        start();
    }
};

// The stall-insurance pump: a coarse re-arming timer driving pump_stalled — throughput itself is reply->refill.
struct pipeline_pump
{
    plexus::freertos::freertos_timer &timer;
    windowed_probe                   &probe;

    void arm()
    {
        timer.expires_after(std::chrono::milliseconds{250});
        timer.async_wait([this](std::error_code ec) { on_tick(ec); });
    }

    void on_tick(std::error_code ec)
    {
        if(ec)
            return;
        probe.pump_stalled();
        arm();
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_ONEWAY_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_ONEWAY_H

#include "bench_workload.h"

#include "plexus/publisher.h"

#include "esp_timer.h"
#include "esp_heap_caps.h"

#include <span>
#include <array>
#include <cstdio>
#include <cstddef>
#include <cstdint>

namespace example {

// Per-tick offered messages and the per-tier measurement window. k_oneway_batch is the bounded batch
// each executor tick publishes before yielding — its value is empirical (tune so dropped() > 0 fires
// The per-tick offer is a fixed BYTE budget (~2x the egress cap), not a fixed message count: a fixed
// count either fails to fill the cap at small payloads (no shed, no saturation witness) or floods it at
// large payloads (a 1 MB/tick publish burst starves the drain so a large frame never completes). Two
// caps' worth per tick fills the 8 KiB cap and sheds the overflow at every tier without flooding.
// After each window a settle interval lets the publish->shed pipeline drain, so a tier's in-flight sheds
// do not leak into the next tier's count (which would drive accepted = offered - dropped negative).
// The offer is sized to the DEFAULT cap and held constant across the cap variants — the egress cap is
// the sole variable under test, so scaling the offer to a deep cap would confound the cap's effect with
// the batch's (a bigger batch starves the drain). At this fixed offer a deep cap simply buffers more
// in-flight before it sheds; the delivered rate is drain-limited either way. The count is bounded so the
// smallest payload cannot explode into a pathological per-tick publish burst.
inline constexpr std::size_t   k_oneway_cap_bytes = 8192;
inline constexpr std::uint32_t k_oneway_max_batch = 1024;
inline constexpr std::int64_t  k_oneway_window_us = 3000000;
inline constexpr std::int64_t  k_oneway_settle_us = 500000;

// Emit the machine-parseable throughput line the runner parses (accepted count/bytes over the window,
// plus the shed witness). Stable space-separated key=value pairs under a BENCH tag, no localized format.
inline void emit_throughput(const char *policy, std::size_t tier, std::uint32_t msgs, std::uint64_t bytes, std::int64_t elapsed_us, std::uint32_t dropped)
{
    std::printf("BENCH throughput policy=%s tier=%u msgs=%u bytes=%llu elapsed_us=%lld dropped=%u\n",
                policy, static_cast<unsigned>(tier), static_cast<unsigned>(msgs), static_cast<unsigned long long>(bytes), static_cast<long long>(elapsed_us), static_cast<unsigned>(dropped));
}

// The saturating one-way offer: each tick publishes a BOUNDED batch of tier-sized messages on "stream"
// then returns, so the run loop drains egress between ticks — never a tight publish() loop, which on
// the single-threaded cooperative executor never yields and sends nothing.
struct oneway_driver
{
    plexus::publisher<void> &stream;
    std::size_t              tier;
    std::array<std::byte, k_max_tier_bytes> buffer{};
    std::uint32_t            offered{0};

    std::uint32_t batch() const
    {
        const std::uint32_t n = static_cast<std::uint32_t>(2 * k_oneway_cap_bytes / tier);
        if(n < 1)
            return 1;
        return n > k_oneway_max_batch ? k_oneway_max_batch : n;
    }

    void offer_batch()
    {
        const std::uint32_t             n = batch();
        const std::span<const std::byte> msg{buffer.data(), tier};
        for(std::uint32_t i = 0; i < n; ++i)
            stream.publish(msg);
        offered += n;
    }
};

// Walks the tiers: each tier offers for a fixed window, then settles (stops offering) so the pipeline
// drains before the counts are read; the accepted rate is accepted = offered - dropped over the window.
struct oneway_runner
{
    const char    *policy;
    oneway_driver &driver;
    std::size_t    tier_index{0};
    std::uint32_t  offered_base{0};
    std::uint32_t  dropped_base{0};
    std::int64_t   window_start{0};
    std::int64_t   settle_start{0};
    bool           offering{true};
    bool           running{true};

    void start()
    {
        driver.tier  = k_payload_tiers[tier_index];
        window_start = esp_timer_get_time();
    }

    void tick(std::uint32_t dropped_total)
    {
        if(!running)
            return;
        const std::int64_t now = esp_timer_get_time();
        if(offering)
        {
            if(now - window_start < k_oneway_window_us)
                return driver.offer_batch();
            offering     = false;
            settle_start = now;
            return;
        }
        if(now - settle_start < k_oneway_settle_us)
            return;
        const std::uint32_t offered  = driver.offered - offered_base;
        const std::uint32_t dropped  = dropped_total - dropped_base;
        const std::uint32_t accepted = offered > dropped ? offered - dropped : 0;
        emit_throughput(policy, driver.tier, accepted, static_cast<std::uint64_t>(accepted) * driver.tier, k_oneway_window_us, dropped);
        if(++tier_index >= k_payload_tiers.size())
        {
            running = false;
            // The free-heap witness at sweep end proves the (possibly deep) egress cap fit alongside the
            // Wi-Fi stack; rx_stack is 0 on the poll-drive one-way path (no RX task).
            emit_resource(policy, 0, esp_get_free_heap_size());
            return;
        }
        offered_base = driver.offered;
        dropped_base = dropped_total;
        driver.tier  = k_payload_tiers[tier_index];
        offering     = true;
        window_start = esp_timer_get_time();
    }
};

// A pollable the run loop drives ONCE per tick: each poll offers one bounded batch (or closes the tier
// window), reading the transport's cumulative shed for the accounting. Because it is one offer per tick
// — not a self-re-arming sub-park timer — the run loop always reaches its park (a >=1 ms idle yield),
// so a saturating stream never starves the task watchdog the way a perpetually-overdue timer does.
template<typename Transport>
struct oneway_source
{
    oneway_runner &runner;
    Transport     &transport;

    void poll()
    {
        runner.tick(transport.dropped());
    }
};

}

#endif

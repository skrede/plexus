#include "support/hold_drive.h"
#include "support/hold_tally.h"
#include "support/hold_topology.h"

#include "plexus/io/upgrade_channel.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <cstdio>
#include <cstdint>
#include <numeric>
#include <algorithm>

using plexus::testing::ring_probe;
using plexus::testing::percentile;
using plexus::testing::hold_now_ns;
using plexus::testing::publish_run;
using plexus::testing::hold_topology;
using plexus::testing::engage_reader;
using plexus::testing::context_thread;
using plexus::testing::publish_paced;
using plexus::testing::attach_newcomer;
using plexus::testing::await_publishes;
using plexus::testing::settle_newcomer;
using plexus::testing::backlog_on_publisher;

namespace {

using namespace std::chrono_literals;

constexpr std::uint32_t k_fill     = 200;
constexpr std::uint32_t k_measured = 1024;
constexpr std::uint32_t k_total    = k_fill + k_measured;

struct run_result
{
    std::uint32_t attempted;
    std::uint32_t would_block;
    std::int64_t publish_total_ns;
    std::int64_t publish_max_ns;
    std::uint64_t newcomer_fallbacks;
    std::int64_t hold_ns;
};

long long us(std::int64_t ns)
{
    return static_cast<long long>(ns / 1000);
}

// A ring write the producer could not make is a publish that surfaced would_block; a run stopped at its deadline did
// not attempt the rest. A warm-up frame still in the lane's bands can reach the ring after the run begins.
run_result measure(hold_topology &t, ring_probe &probe, const publish_run &run, std::uint32_t direct, std::uint64_t tail_before)
{
    const plexus::io::fallback_counts fallbacks = t.newcomer.node.same_host_fallbacks();
    const auto attempted                        = static_cast<std::uint32_t>(direct + run.durations.size());
    return {.attempted          = attempted,
            .would_block        = attempted - std::min(attempted, static_cast<std::uint32_t>(probe.tail() - tail_before)),
            .publish_total_ns   = std::accumulate(run.durations.begin(), run.durations.end(), std::int64_t{0}),
            .publish_max_ns     = run.durations.empty() ? 0 : std::ranges::max(run.durations),
            .newcomer_fallbacks = std::accumulate(fallbacks.begin(), fallbacks.end(), std::uint64_t{0}),
            .hold_ns            = 0};
}

void report(const char *arm, std::chrono::nanoseconds interval, hold_topology &t, const run_result &r)
{
    std::vector<std::int64_t> lat = t.reader_tally.latencies();
    const auto &rd                = t.reader_tally;
    const auto &nc                = t.newcomer_tally;
    std::printf("switch_hold %s interval_ns=%lld published=%u attempted=%u reader: delivered=%u dropped=%u dup=%u ooo=%u gap_max_us=%lld p50_us=%lld p99_us=%lld p999_us=%lld "
                "max_us=%lld producer: total_ms=%lld max_us=%lld would_block=%u newcomer: delivered=%u dup=%u ooo=%u wire_dropped=%u fallbacks=%llu hold_us=%lld\n",
                arm, static_cast<long long>(interval.count()), k_total, r.attempted, rd.delivered(), k_total - rd.delivered(), rd.duplicates(), rd.out_of_order(), us(rd.max_gap_ns()),
                us(percentile(lat, 0.50)), us(percentile(lat, 0.99)), us(percentile(lat, 0.999)), us(percentile(lat, 1.0)), us(r.publish_total_ns) / 1000, us(r.publish_max_ns),
                r.would_block, nc.delivered(), nc.duplicates(), nc.out_of_order(), t.drops.count(), static_cast<unsigned long long>(r.newcomer_fallbacks), us(r.hold_ns));
    std::fflush(stdout);
}

void require_counts(hold_topology &t, const run_result &r)
{
    CHECK(t.reader_tally.duplicates() == 0);
    CHECK(t.reader_tally.out_of_order() == 0);
    CHECK(t.newcomer_tally.duplicates() == 0);
    CHECK(t.newcomer_tally.out_of_order() == 0);
    CHECK(r.attempted == k_total);
    CHECK(t.reader_tally.delivered() == k_total);
    CHECK(r.would_block == 0);
    CHECK(t.newcomer_tally.delivered() + t.drops.count() == k_total);
}

// A run that misses its deadline is already lost; freeing the newcomer's cursor lets the publish inside the producer end.
void finish_publishes(ring_probe &probe, publish_run &run, std::uint32_t count, std::uint32_t newcomer_cursor)
{
    if(!await_publishes(run, count, 10s))
        probe.release(newcomer_cursor);
    plexus::testing::await_stopped(run, count);
    std::this_thread::sleep_for(1s);
}

// The newcomer is left unread from its attach on; the producer fills its wire before reading its ready, so it can
// never switch it while the reader takes the measured publishes from the ring.
void run_backed_up(std::chrono::nanoseconds interval)
{
    hold_topology t{k_total, k_fill};
    ring_probe probe;
    REQUIRE(engage_reader(t, probe));
    const std::uint32_t reader_cursor = probe.cursor_besides({});
    REQUIRE(attach_newcomer(t));
    const std::uint32_t cursor      = probe.cursor_besides({reader_cursor});
    const std::uint64_t tail_before = probe.tail();
    for(std::uint32_t index = 0; index < k_fill; ++index)
        t.publish(index);
    publish_run run;
    {
        context_thread publisher{t.publisher};
        context_thread reader{t.reader};
        std::this_thread::sleep_for(50ms);
        REQUIRE(backlog_on_publisher(t) > 0);
        publish_paced(t, run, k_fill, k_measured, interval);
        finish_publishes(probe, run, k_measured, cursor);
    }
    run_result r = measure(t, probe, run, k_fill, tail_before);
    settle_newcomer(t);
    r.newcomer_fallbacks = measure(t, probe, run, k_fill, tail_before).newcomer_fallbacks;
    report("backed_up", interval, t, r);
    require_counts(t, r);
}

// The time from the newcomer's attach until its cursor first moves, which its switch starts.
std::int64_t watch_hold(ring_probe &probe, std::uint32_t cursor, std::int64_t attached_ns, const std::atomic<bool> &stop)
{
    const std::uint64_t at_attach = probe.position(cursor);
    while(!stop.load(std::memory_order_relaxed))
        if(probe.position(cursor) != at_attach)
            return plexus::testing::hold_now_ns() - attached_ns;
    return -1;
}

void run_control(std::chrono::nanoseconds interval)
{
    hold_topology t{k_total, k_fill};
    ring_probe probe;
    REQUIRE(engage_reader(t, probe));
    const std::uint32_t reader_cursor = probe.cursor_besides({});
    REQUIRE(attach_newcomer(t));
    const std::int64_t attached_ns  = hold_now_ns();
    const std::uint32_t cursor      = probe.cursor_besides({reader_cursor});
    const std::uint64_t tail_before = probe.tail();
    publish_run run;
    std::atomic<bool> stop{false};
    std::int64_t hold_ns = -1;
    {
        std::thread watcher{[&] { hold_ns = watch_hold(probe, cursor, attached_ns, stop); }};
        context_thread newcomer{t.newcomer};
        context_thread publisher{t.publisher};
        context_thread reader{t.reader};
        publish_paced(t, run, 0, k_total, interval);
        finish_publishes(probe, run, k_total, cursor);
        stop.store(true);
        watcher.join();
    }
    run_result r = measure(t, probe, run, 0, tail_before);
    settle_newcomer(t);
    r.hold_ns = hold_ns;
    report("control", interval, t, r);
    require_counts(t, r);
}

// The shortest interval of the control's doubling sweep at which every loaded control run lost nothing.
constexpr std::chrono::nanoseconds k_paced{2000};

}

TEST_CASE("switch_hold a reader engaged beside a newcomer whose wire is backed up loses nothing when publishes come back to back", "[shm][switch_hold]")
{
    run_backed_up(0ns);
}

TEST_CASE("switch_hold a reader engaged beside a newcomer whose wire is backed up loses nothing when publishes are paced", "[shm][switch_hold]")
{
    run_backed_up(k_paced);
}

TEST_CASE("switch_hold control a newcomer that switches normally while publishes come back to back", "[shm][switch_hold]")
{
    run_control(0ns);
}

TEST_CASE("switch_hold control a newcomer that switches normally while publishes are paced", "[shm][switch_hold]")
{
    run_control(k_paced);
}

TEST_CASE("switch_hold control interval sweep", "[.sweep][shm][switch_hold]")
{
    for(std::chrono::nanoseconds interval = 1000ns; interval <= 1024000ns; interval *= 2)
        run_control(interval);
}

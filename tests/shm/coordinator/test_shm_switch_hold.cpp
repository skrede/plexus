#include "support/hold_drive.h"
#include "support/hold_tally.h"
#include "support/hold_topology.h"

#include <catch2/catch_test_macros.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <cstdio>
#include <cstdint>

using plexus::testing::measure;
using plexus::testing::ring_probe;
using plexus::testing::percentile;
using plexus::testing::run_result;
using plexus::testing::hold_now_ns;
using plexus::testing::publish_run;
using plexus::testing::hold_drivers;
using plexus::testing::hold_topology;
using plexus::testing::engage_reader;
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

long long us(std::int64_t ns)
{
    return static_cast<long long>(ns / 1000);
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
    run_result r{};
    {
        hold_drivers drivers{t};
        drivers.drive_publisher();
        drivers.drive_reader();
        std::this_thread::sleep_for(50ms);
        REQUIRE(backlog_on_publisher(t) > 0);
        publish_paced(t, run, k_fill, k_measured, interval);
        finish_publishes(probe, run, k_measured, cursor);
        r = measure(probe, run, k_fill, tail_before);
        drivers.drive_newcomer();
        settle_newcomer(t);
        r.newcomer_fallbacks = plexus::testing::newcomer_fallbacks(t);
    }
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
    run_result r{};
    std::atomic<bool> stop{false};
    std::int64_t hold_ns = -1;
    {
        std::thread watcher{[&] { hold_ns = watch_hold(probe, cursor, attached_ns, stop); }};
        hold_drivers drivers{t};
        drivers.drive_newcomer();
        drivers.drive_publisher();
        drivers.drive_reader();
        publish_paced(t, run, 0, k_total, interval);
        finish_publishes(probe, run, k_total, cursor);
        stop.store(true);
        watcher.join();
        r                    = measure(probe, run, 0, tail_before);
        r.newcomer_fallbacks = plexus::testing::newcomer_fallbacks(t);
        settle_newcomer(t);
    }
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

#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVE_H

#include "support/hold_tally.h"
#include "support/hold_drivers.h"
#include "support/hold_topology.h"

#include "plexus/io/upgrade_channel.h"

#include <asio/post.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <algorithm>

namespace plexus::testing {

// The publishes of one run, posted onto the publisher's executor and timed there.
struct publish_run
{
    publish_run()
            : stop(false)
            , done(0)
            , durations()
    {
    }

    std::atomic<bool> stop;
    std::atomic<std::uint32_t> done;
    std::vector<std::int64_t> durations;
};

inline void publish_timed(hold_topology &t, publish_run &run, std::uint32_t index)
{
    if(!run.stop.load(std::memory_order_relaxed))
    {
        const std::int64_t start = hold_now_ns();
        t.publish(index);
        run.durations.push_back(hold_now_ns() - start);
    }
    run.done.fetch_add(1, std::memory_order_release);
}

// Back to back, each publish posts the next, so the publisher's executor runs its I/O between two publishes.
inline void post_chain(hold_topology &t, publish_run &run, std::uint32_t index, std::uint32_t end)
{
    ::asio::post(t.publisher.io,
                 [&t, &run, index, end]
                 {
                     publish_timed(t, run, index);
                     if(index + 1 < end)
                         post_chain(t, run, index + 1, end);
                 });
}

// A zero interval publishes back to back; otherwise each publish is posted at its time.
inline void publish_paced(hold_topology &t, publish_run &run, std::uint32_t first, std::uint32_t count, std::chrono::nanoseconds interval)
{
    if(interval.count() == 0)
        return post_chain(t, run, first, first + count);
    const auto start = std::chrono::steady_clock::now();
    for(std::uint32_t i = 0; i < count; ++i)
    {
        while(std::chrono::steady_clock::now() < start + interval * i)
            std::this_thread::yield();
        ::asio::post(t.publisher.io, [&t, &run, index = first + i] { publish_timed(t, run, index); });
    }
}

// A run still publishing at the deadline stops; false then, and the caller frees whatever holds the producer before the
// publish already inside it can return.
inline bool await_publishes(publish_run &run, std::uint32_t count, std::chrono::seconds deadline)
{
    const auto end = std::chrono::steady_clock::now() + deadline;
    while(run.done.load(std::memory_order_acquire) < count && std::chrono::steady_clock::now() < end)
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    run.stop.store(true, std::memory_order_relaxed);
    return run.done.load(std::memory_order_acquire) == count;
}

inline void await_stopped(publish_run &run, std::uint32_t count)
{
    while(run.done.load(std::memory_order_acquire) < count)
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
}

struct run_result
{
    std::uint32_t attempted;
    std::uint32_t would_block;
    std::int64_t publish_total_ns;
    std::int64_t publish_max_ns;
    std::uint64_t newcomer_fallbacks;
    std::int64_t hold_ns;
};

// A ring write the producer could not make is a publish that surfaced would_block; a run stopped at its deadline did
// not attempt the rest. A warm-up frame still in the lane's bands can reach the ring after the run begins.
inline run_result measure(ring_probe &probe, const publish_run &run, std::uint32_t direct, std::uint64_t tail_before)
{
    const auto attempted = static_cast<std::uint32_t>(direct + run.durations.size());
    return {.attempted          = attempted,
            .would_block        = attempted - std::min(attempted, static_cast<std::uint32_t>(probe.tail() - tail_before)),
            .publish_total_ns   = std::accumulate(run.durations.begin(), run.durations.end(), std::int64_t{0}),
            .publish_max_ns     = run.durations.empty() ? 0 : std::ranges::max(run.durations),
            .newcomer_fallbacks = 0,
            .hold_ns            = 0};
}

// The publisher's wire backlog toward the newcomer, read on the publisher's executor while its thread runs it.
inline std::size_t backlog_on_publisher(hold_topology &t)
{
    return read_on(t.publisher.io, [&t] { return t.newcomer_backlog(); });
}

inline std::uint32_t newcomer_seen(hold_topology &t)
{
    return read_on(t.newcomer.io, [&t] { return t.newcomer_tally.delivered() + t.newcomer_tally.duplicates(); });
}

// Every node runs on its own thread until the newcomer has taken what reaches it and its wire is empty.
inline void settle_newcomer(hold_topology &t)
{
    std::uint32_t seen = newcomer_seen(t);
    auto quiet_since   = std::chrono::steady_clock::now();
    const auto give_up = quiet_since + std::chrono::seconds{20};
    while(std::chrono::steady_clock::now() - quiet_since < std::chrono::milliseconds{500} && std::chrono::steady_clock::now() < give_up)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
        const std::uint32_t now_seen = newcomer_seen(t);
        if(now_seen != seen || backlog_on_publisher(t) != 0)
            quiet_since = std::chrono::steady_clock::now();
        seen = now_seen;
    }
}

inline std::uint64_t newcomer_fallbacks(hold_topology &t)
{
    const plexus::io::fallback_counts counts = read_on(t.newcomer.io, [&t] { return t.newcomer.node.same_host_fallbacks(); });
    return std::accumulate(counts.begin(), counts.end(), std::uint64_t{0});
}

}

#endif

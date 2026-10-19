#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVE_H

#include "support/hold_tally.h"
#include "support/co_host_node.h"
#include "support/hold_topology.h"

#include <asio/post.hpp>
#include <asio/executor_work_guard.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::testing {

// Runs one node's io_context on its own thread for as long as it lives.
class context_thread
{
public:
    explicit context_thread(co_host_node &node)
            : m_node(node)
            , m_guard(::asio::make_work_guard(node.io))
            , m_thread(
                      [&node]
                      {
                          node.io.restart();
                          node.io.run();
                      })
    {
    }

    ~context_thread()
    {
        m_guard.reset();
        m_node.io.stop();
        m_thread.join();
    }

private:
    co_host_node &m_node;
    ::asio::executor_work_guard<::asio::io_context::executor_type> m_guard;
    std::thread m_thread;
};

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

// Every node runs on the calling thread until the newcomer has taken what reaches it and its wire is empty.
inline void settle_newcomer(hold_topology &t)
{
    std::uint32_t seen = t.newcomer_tally.delivered() + t.newcomer_tally.duplicates();
    auto quiet_since   = std::chrono::steady_clock::now();
    const auto give_up = quiet_since + std::chrono::seconds{20};
    while(std::chrono::steady_clock::now() - quiet_since < std::chrono::milliseconds{500} && std::chrono::steady_clock::now() < give_up)
    {
        t.publisher.run_for(std::chrono::milliseconds{2});
        t.reader.run_for(std::chrono::milliseconds{1});
        t.newcomer.run_for(std::chrono::milliseconds{2});
        const std::uint32_t now_seen = t.newcomer_tally.delivered() + t.newcomer_tally.duplicates();
        if(now_seen != seen || t.newcomer_backlog() != 0)
            quiet_since = std::chrono::steady_clock::now();
        seen = now_seen;
    }
}

// The publisher's wire backlog toward the newcomer, read on the publisher's executor while its thread runs it.
inline std::size_t backlog_on_publisher(hold_topology &t)
{
    std::atomic<std::size_t> backlog{0};
    std::atomic<bool> read{false};
    ::asio::post(t.publisher.io,
                 [&]
                 {
                     backlog.store(t.newcomer_backlog());
                     read.store(true, std::memory_order_release);
                 });
    while(!read.load(std::memory_order_acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
    return backlog.load();
}

}

#endif

// Times the macOS shm notifier's wake end to end: producer signal() -> the parked waiter's own
// named semaphore -> waiter thread -> process-local self-pipe doorbell -> reactor -> posted drain.

#include "plexus/asio/shm/macos/sem_notifier.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/region_naming.h"

#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <algorithm>

#include <unistd.h>

namespace {

using clock_type = std::chrono::steady_clock;

constexpr std::size_t k_warmup = 512;
constexpr std::size_t k_iters  = 5000;
constexpr std::size_t k_runs   = 5;

struct bench_policy
{
    using executor_type = ::asio::io_context &;
    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ::asio::post(ex, std::move(fn));
    }
};

double percentile(std::vector<double> &sorted, double q)
{
    if(sorted.empty())
        return 0.0;
    const auto rank = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
    return sorted[rank];
}

// A producer wakes only a parked waiter with a registered cursor, so the header registers slot 0
// and every timed signal waits for the waiter to park first.
::plexus::shm::control_header_t &one_waiter(::plexus::shm::control_header_t &header)
{
    header.consumer_capacity = 1;
    header.cursors[0].active.store(1, std::memory_order_release);
    header.high_water.store(1, std::memory_order_release);
    return header;
}

// Owns one notifier instance and the single-threaded reactor that drains its doorbell. The
// notifier's own waiter thread does the sem_wait off this thread; run_one drives the drain here.
template<typename Notifier>
class wake_probe
{
public:
    wake_probe()
            : m_io()
            , m_header()
            , m_notifier(m_io, one_waiter(m_header), ::plexus::shm::bare_hex(static_cast<std::uint64_t>(::getpid())))
            , m_drained(0)
            , m_drain_time()
    {
        m_notifier.arm(0, [this] { m_drain_time = clock_type::now(); ++m_drained; });
    }

    bool measure(std::size_t warmup, std::size_t iters, std::vector<double> &out)
    {
        if(!pump_to(m_drained + 1)) // consume the arm-time startup poke before timing
            return false;
        for(std::size_t i = 0; i < warmup + iters; ++i)
        {
            const auto target = m_drained + 1;
            while(m_header.park[0].load(std::memory_order_seq_cst) != ::plexus::shm::k_park_parked)
                std::this_thread::yield();
            const auto t0 = clock_type::now();
            m_notifier.signal();
            if(!pump_to(target))
                return false;
            if(i >= warmup)
                out.push_back(std::chrono::duration<double, std::micro>(m_drain_time - t0).count());
        }
        return true;
    }

private:
    bool pump_to(std::uint64_t target)
    {
        while(m_drained < target)
            if(m_io.run_one_for(std::chrono::seconds(5)) == 0)
                return false; // the notifier never woke -- report rather than hang
        return true;
    }

    ::asio::io_context              m_io;
    ::plexus::shm::control_header_t m_header;
    Notifier                        m_notifier;
    std::uint64_t                   m_drained;
    clock_type::time_point          m_drain_time;
};

struct run_stats
{
    std::vector<double> per_run_p50;
    std::vector<double> per_run_p99;
    std::vector<double> all_samples;
    bool                ran{false};
};

template<typename Notifier>
run_stats run_variant()
{
    run_stats r;
    for(std::size_t run = 0; run < k_runs; ++run)
    {
        std::vector<double> samples;
        wake_probe<Notifier> probe;
        if(!probe.measure(k_warmup, k_iters, samples))
        {
            std::cerr << "notifier stalled on run " << run << " -- no wake within timeout\n";
            return r; // ran stays false: an unrunnable probe is reported, not faked
        }
        std::sort(samples.begin(), samples.end());
        r.per_run_p50.push_back(percentile(samples, 0.50));
        r.per_run_p99.push_back(percentile(samples, 0.99));
        r.all_samples.insert(r.all_samples.end(), samples.begin(), samples.end());
    }
    r.ran = true;
    return r;
}

void emit(std::ostream &out, const run_stats &r)
{
    out << "# macOS shm notifier wake-latency (per-waiter semaphore, single process)\n\n";
    out << "Host: Apple Silicon arm64, AppleClang all-options build. " << k_runs << " runs x "
        << k_iters << " timed iterations (" << k_warmup << " warmup discarded).\n";
    out << "Latency is end-to-end signal() -> drain (steady_clock): the parked waiter's own semaphore ->\n"
        << "waiter wakeup -> self-pipe doorbell -> reactor drain -> posted drain.\n\n";
    if(!r.ran)
    {
        out << "The probe did not run on this host; no numbers recorded.\n";
        return;
    }
    out << "| run | p50 (us) | p99 (us) |\n| --- | -------- | -------- |\n";
    for(std::size_t i = 0; i < r.per_run_p50.size(); ++i)
    {
        char line[96];
        std::snprintf(line, sizeof line, "| %zu | %8.3f | %8.3f |\n", i, r.per_run_p50[i], r.per_run_p99[i]);
        out << line;
    }
    std::vector<double> all = r.all_samples;
    std::sort(all.begin(), all.end());
    char agg[128];
    std::snprintf(agg, sizeof agg, "\naggregate p50 = %.3f us, p99 = %.3f us (n=%zu)\n",
                  percentile(all, 0.50), percentile(all, 0.99), all.size());
    out << agg;
}

}

int main(int argc, char **argv)
{
    namespace pas = plexus::asio::shm;

    const run_stats r = run_variant<pas::sem_notifier<bench_policy>>();

    emit(std::cout, r);

    const std::string path = argc > 1 ? argv[1] : "notifier_wake_latency_report.md";
    if(std::ofstream file{path})
    {
        emit(file, r);
        std::cerr << "\nreport written to " << path << '\n';
    }

    return r.ran ? 0 : 1;
}

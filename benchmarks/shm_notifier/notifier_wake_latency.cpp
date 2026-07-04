// A wake-latency probe for the macOS shm notifier. It times the full path producer signal() ->
// sem_post -> waiter thread wakes -> pokes the process-local self-pipe doorbell -> reactor drains
// -> posted drain runs. The head-to-head that once measured a second private-kqueue EVFILT_USER
// doorbell variant found the two doorbells latency-indistinguishable (the ~16.8 us wake is
// dominated by the shared named-semaphore leg; the marginal doorbell delta was within timer
// noise), so the mechanism collapsed to the self-pipe and this stays as a single-variant probe.
//
// The waiter loop pokes the doorbell once at arm (drain-before-wait), so the first poke is consumed
// before timing begins; thereafter each signal() yields exactly one doorbell poke because the
// harness waits for the drain before the next signal().

#include "plexus/asio/shm/macos/sem_notifier.h"

#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <algorithm>

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

// Owns one notifier instance and the single-threaded reactor that drains its doorbell. The
// notifier's own waiter thread does the sem_wait off this thread; run_one drives the drain here.
template<typename Notifier>
class wake_probe
{
public:
    wake_probe()
            : m_notifier(m_io, m_word)
    {
        m_notifier.arm([this] { m_drain_time = clock_type::now(); ++m_drained; });
    }

    bool measure(std::size_t warmup, std::size_t iters, std::vector<double> &out)
    {
        if(!pump_to(m_drained + 1)) // consume the arm-time startup poke before timing
            return false;
        for(std::size_t i = 0; i < warmup + iters; ++i)
        {
            const auto target = m_drained + 1;
            const auto t0     = clock_type::now();
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

    ::asio::io_context         m_io;
    std::atomic<std::uint32_t> m_word{0};
    Notifier                   m_notifier;
    std::uint64_t              m_drained{0};
    clock_type::time_point     m_drain_time{};
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
    out << "# macOS shm notifier wake-latency (self-pipe doorbell, single process)\n\n";
    out << "Host: Apple Silicon arm64, AppleClang all-options build. " << k_runs << " runs x "
        << k_iters << " timed iterations (" << k_warmup << " warmup discarded).\n";
    out << "Latency is end-to-end signal() -> drain (steady_clock): sem_post -> waiter wakeup ->\n"
        << "self-pipe doorbell -> reactor drain -> posted drain. The ~16.8 us floor is the shared\n"
        << "named-semaphore + cross-thread wakeup leg; the local doorbell is a negligible fraction.\n\n";
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

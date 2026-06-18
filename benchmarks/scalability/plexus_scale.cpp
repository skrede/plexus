// A self-contained, single-process scalability probe for plexus over the header-only
// in-process backend. It sweeps nodes x topics x payload and reports, per cell, the
// setup wall-time (construct all nodes + endpoints to readiness), the steady-state
// one-way delivery latency (p50/p99, a steady_clock stamp carried in the payload), and
// the process footprint (VmRSS, open fds, threads) read from /proc/self. It is the
// plexus reference half of an eventual cross-middleware scalability comparison: it
// measures only plexus's own cost against its own axes, no field middlewares.
//
// Topology per cell: one eager subscriber/aggregator node and (nodes-1) lazy publisher
// nodes share one bus, executor, and discovery table. The subscriber node holds one
// subscriber per topic; each publisher node holds one publisher per topic. A subscriber
// on a topic therefore receives from every publisher on that topic, so the matched
// (node, topic) fan-in is what scales. Built entirely through the public endpoint API
// (the wiring mirrors examples/typed_inproc_fastpath.cpp).

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <array>
#include <deque>
#include <chrono>
#include <cstdio>
#include <limits>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <utility>
#include <iostream>
#include <algorithm>
#include <filesystem>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using clock_type = std::chrono::steady_clock;
using node_type = plexus::node<inproc_policy, inproc_transport<>>;

struct footprint
{
    std::uint64_t rss_kib{};
    std::uint64_t open_fds{};
    std::uint64_t threads{};
};

std::uint64_t read_vmrss_kib()
{
    std::ifstream status{"/proc/self/status"};
    std::string key;
    while(status >> key)
    {
        if(key == "VmRSS:")
        {
            std::uint64_t value = 0;
            status >> value;
            return value;
        }
        status.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return 0;
}

std::uint64_t count_dir_entries(const char *path)
{
    std::error_code ec;
    std::filesystem::directory_iterator it{path, ec};
    if(ec)
        return 0;
    return static_cast<std::uint64_t>(std::distance(begin(it), end(it)));
}

footprint sample_footprint()
{
    return {read_vmrss_kib(), count_dir_entries("/proc/self/fd"), count_dir_entries("/proc/self/task")};
}

struct latency
{
    double p50_us{};
    double p99_us{};
    std::uint64_t delivered{};
};

double percentile(std::vector<double> &sorted, double q)
{
    if(sorted.empty())
        return 0.0;
    const auto rank = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
    return sorted[rank];
}

latency reduce(std::vector<double> &samples_us)
{
    std::sort(samples_us.begin(), samples_us.end());
    return {percentile(samples_us, 0.50), percentile(samples_us, 0.99),
            static_cast<std::uint64_t>(samples_us.size())};
}

plexus::node_options publisher_opts(std::uint64_t seed)
{
    plexus::node_options o;
    o.redial_seed = seed;
    o.dial_eagerly = false;
    return o;
}

plexus::node_options subscriber_opts()
{
    plexus::node_options o;
    o.redial_seed = 0x5;
    o.dial_eagerly = true;
    return o;
}

// One cell's live graph. The subscriber node (index 0) is the single eager dialer; the
// remaining nodes are lazy publishers it dials once it is aware of them through the
// shared discovery table. Nodes and transports are non-movable, so they live in deques
// whose element addresses never shift as the graph grows.
struct mesh
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};
    std::deque<inproc_transport<>> transports;
    std::deque<node_type> nodes;

    explicit mesh(std::size_t node_count)
    {
        for(std::size_t i = 0; i < node_count; ++i)
        {
            auto &t = transports.emplace_back(ex, bus);
            const bool is_subscriber = i == 0;
            auto opts = is_subscriber ? subscriber_opts() : publisher_opts(0x100 + i);
            nodes.emplace_back(ex, disc, "scale-node-" + std::to_string(i), t, opts);
        }
        for(std::size_t i = 0; i < node_count; ++i)
            nodes[i].listen({"inproc", "host-" + std::to_string(i) + ":" + std::to_string(5000 + i)});
        ex.drain();
    }
};

std::string topic_name(std::size_t t)
{
    return "scale/topic/" + std::to_string(t);
}

void write_stamp(std::vector<std::byte> &payload)
{
    const auto now = static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
    for(std::size_t i = 0; i < sizeof now; ++i)
        payload[i] = static_cast<std::byte>((now >> (8 * i)) & 0xff);
}

std::uint64_t read_stamp(std::span<const std::byte> bytes)
{
    std::uint64_t v = 0;
    for(std::size_t i = 0; i < sizeof v; ++i)
        v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[i])) << (8 * i);
    return v;
}

struct cell_result
{
    double setup_ms{};
    latency lat;
    footprint feet;
};

// Build the cell, settle it, drive a fixed message count per (publisher, topic) pair,
// and reduce the captured one-way deltas. Each subscriber callback reads the receive
// clock and records (now - sent) in microseconds against the carried stamp.
cell_result run_cell(std::size_t node_count, std::size_t topic_count,
                     std::size_t payload_bytes, std::uint64_t messages_per_pair)
{
    std::vector<double> samples_us;

    const auto t_setup0 = clock_type::now();
    mesh m{node_count};

    std::deque<plexus::subscriber<>> subs;
    for(std::size_t t = 0; t < topic_count; ++t)
        subs.emplace_back(m.nodes[0], topic_name(t), [&samples_us](std::span<const std::byte> bytes) {
            const auto recv = static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
            const double delta_ns = static_cast<double>(recv - read_stamp(bytes));
            samples_us.push_back(delta_ns / 1000.0);
        });

    std::deque<plexus::publisher<>> pubs;
    for(std::size_t n = 1; n < node_count; ++n)
        for(std::size_t t = 0; t < topic_count; ++t)
            pubs.emplace_back(m.nodes[n], topic_name(t));
    m.ex.drain();
    const double setup_ms = std::chrono::duration<double, std::milli>(clock_type::now() - t_setup0).count();

    std::vector<std::byte> payload(std::max<std::size_t>(payload_bytes, sizeof(std::uint64_t)));
    for(std::uint64_t i = 0; i < messages_per_pair; ++i)
        for(auto &p : pubs)
        {
            write_stamp(payload);
            p.publish(std::span<const std::byte>{payload});
            m.ex.drain();
        }

    return {setup_ms, reduce(samples_us), sample_footprint()};
}

const std::array<std::size_t, 4> g_nodes = {1, 4, 16, 64};
const std::array<std::size_t, 4> g_topics = {1, 4, 16, 64};
const std::array<std::size_t, 2> g_payloads = {8, 4096};
constexpr std::uint64_t g_messages_per_pair = 200;

void print_matrix(std::ostream &out, std::size_t payload,
                  const std::vector<std::vector<cell_result>> &grid)
{
    out << "\n### p50 one-way latency (us) -- payload " << payload << " B\n\n";
    out << "| nodes \\ topics |";
    for(auto t : g_topics)
        out << ' ' << t << " |";
    out << "\n|---|";
    for(std::size_t i = 0; i < g_topics.size(); ++i)
        out << "---|";
    out << '\n';
    for(std::size_t ni = 0; ni < g_nodes.size(); ++ni)
    {
        out << "| **" << g_nodes[ni] << "** |";
        for(std::size_t ti = 0; ti < g_topics.size(); ++ti)
        {
            const auto &lat = grid[ni][ti].lat;
            if(lat.delivered == 0)
            {
                out << " n/a |";
                continue;
            }
            char buf[32];
            std::snprintf(buf, sizeof buf, " %.3f |", lat.p50_us);
            out << buf;
        }
        out << '\n';
    }
}

void print_footprint_table(std::ostream &out, const char *title, const char *unit,
                           const std::vector<std::vector<cell_result>> &grid,
                           std::uint64_t (*pick)(const footprint &))
{
    out << "\n### " << title << " (" << unit << ") -- rows nodes, cols topics\n\n";
    out << "| nodes \\ topics |";
    for(auto t : g_topics)
        out << ' ' << t << " |";
    out << "\n|---|";
    for(std::size_t i = 0; i < g_topics.size(); ++i)
        out << "---|";
    out << '\n';
    for(std::size_t ni = 0; ni < g_nodes.size(); ++ni)
    {
        out << "| **" << g_nodes[ni] << "** |";
        for(std::size_t ti = 0; ti < g_topics.size(); ++ti)
            out << ' ' << pick(grid[ni][ti].feet) << " |";
        out << '\n';
    }
}

void print_setup_table(std::ostream &out, const std::vector<std::vector<cell_result>> &grid)
{
    out << "\n### setup wall-time (ms) -- rows nodes, cols topics\n\n";
    out << "| nodes \\ topics |";
    for(auto t : g_topics)
        out << ' ' << t << " |";
    out << "\n|---|";
    for(std::size_t i = 0; i < g_topics.size(); ++i)
        out << "---|";
    out << '\n';
    for(std::size_t ni = 0; ni < g_nodes.size(); ++ni)
    {
        out << "| **" << g_nodes[ni] << "** |";
        for(std::size_t ti = 0; ti < g_topics.size(); ++ti)
        {
            char buf[32];
            std::snprintf(buf, sizeof buf, " %.3f |", grid[ni][ti].setup_ms);
            out << buf;
        }
        out << '\n';
    }
}

void emit_report(std::ostream &out,
                 const std::vector<std::vector<std::vector<cell_result>>> &by_payload,
                 std::uint64_t peak_rss_kib)
{
    out << "# plexus scalability first-cut (inproc, single process)\n\n";
    out << "Axes: nodes " << "{1,4,16,64}, topics {1,4,16,64}, payload {8,4096} B; "
        << g_messages_per_pair << " messages per (publisher, topic) pair.\n";
    out << "Latency is one-way (steady_clock stamp in payload) over the in-process bus, "
        << "single executor. Footprint is the whole-process VmRSS/fd/thread sampled after "
        << "the cell's steady loop.\n";
    out << "\nPeak process VmRSS observed across the sweep: " << peak_rss_kib << " KiB.\n";
    out << "\nThe nodes=1 row carries no publisher peers (publishers live on nodes 1..N-1), so "
        << "its delivery latency is n/a by construction; it still exercises subscriber setup + "
        << "footprint.\n";
    for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
        print_matrix(out, g_payloads[pi], by_payload[pi]);
    const auto &heaviest = by_payload.back();
    print_setup_table(out, heaviest);
    print_footprint_table(out, "VmRSS", "KiB", heaviest, [](const footprint &f) { return f.rss_kib; });
    print_footprint_table(out, "open fds", "count", heaviest, [](const footprint &f) { return f.open_fds; });
    print_footprint_table(out, "threads", "count", heaviest, [](const footprint &f) { return f.threads; });
}

}

int main(int argc, char **argv)
{
    std::vector<std::vector<std::vector<cell_result>>> by_payload(g_payloads.size());
    std::uint64_t peak_rss_kib = 0;
    for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
        for(auto nodes : g_nodes)
        {
            by_payload[pi].emplace_back();
            for(auto topics : g_topics)
            {
                auto r = run_cell(nodes, topics, g_payloads[pi], g_messages_per_pair);
                peak_rss_kib = std::max(peak_rss_kib, r.feet.rss_kib);
                by_payload[pi].back().push_back(r);
            }
        }

    emit_report(std::cout, by_payload, peak_rss_kib);

    const std::string path = argc > 1 ? argv[1] : "plexus_scale_report.md";
    std::ofstream file{path};
    if(file)
    {
        emit_report(file, by_payload, peak_rss_kib);
        std::cerr << "\nreport written to " << path << '\n';
    }
    return 0;
}

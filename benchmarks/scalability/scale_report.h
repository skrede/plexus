#ifndef HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_REPORT_H
#define HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_REPORT_H

#include "scale_cell.h"
#include "scale_footprint.h"

#include <span>
#include <array>
#include <cstdio>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <string_view>

namespace scale {

using cell_grid = std::vector<std::vector<cell_result>>; // rows nodes, cols topics

struct sweep_axes
{
    std::uint64_t messages_per_pair{};
    std::span<const std::size_t> nodes;
    std::span<const std::size_t> topics;
    std::span<const std::size_t> payloads;
};

struct sweep_results
{
    std::uint64_t peak_rss_kib{};
    std::vector<cell_grid> by_payload;
};

inline void put_fixed3(std::ostream &out, double v)
{
    std::array<char, 32> buf{};
    std::snprintf(buf.data(), buf.size(), " %.3f |", v);
    out << buf.data();
}

template<typename Cell>
void print_table(std::ostream &out, const sweep_axes &axes, const cell_grid &grid, Cell cell)
{
    out << "| nodes \\ topics |";
    for(auto t : axes.topics)
        out << ' ' << t << " |";
    out << "\n|---|";
    for(std::size_t i = 0; i < axes.topics.size(); ++i)
        out << "---|";
    out << '\n';
    for(std::size_t ni = 0; ni < axes.nodes.size(); ++ni)
    {
        out << "| **" << axes.nodes[ni] << "** |";
        for(std::size_t ti = 0; ti < axes.topics.size(); ++ti)
            cell(grid[ni][ti]);
        out << '\n';
    }
}

inline void print_latency(std::ostream &out, const latency &lat)
{
    if(lat.delivered == 0)
    {
        out << " n/a |";
        return;
    }
    put_fixed3(out, lat.p50_us);
}

inline void print_matrix(std::ostream &out, const sweep_axes &axes, std::size_t payload, const cell_grid &grid)
{
    out << "\n### p50 one-way latency (us) -- payload " << payload << " B\n\n";
    print_table(out, axes, grid, [&out](const cell_result &c) { print_latency(out, c.lat); });
}

inline void print_setup_table(std::ostream &out, const sweep_axes &axes, const cell_grid &grid)
{
    out << "\n### setup wall-time (ms) -- rows nodes, cols topics\n\n";
    print_table(out, axes, grid, [&out](const cell_result &c) { put_fixed3(out, c.setup_ms); });
}

inline void print_footprint_table(std::ostream &out, const sweep_axes &axes, std::string_view title, std::string_view unit, const cell_grid &grid, std::uint64_t footprint::*field)
{
    out << "\n### " << title << " (" << unit << ") -- rows nodes, cols topics\n\n";
    print_table(out, axes, grid, [&out, field](const cell_result &c) { out << ' ' << c.feet.*field << " |"; });
}

inline void print_preamble(std::ostream &out, std::uint64_t messages_per_pair, std::uint64_t peak_rss_kib)
{
    out << "# plexus scalability first-cut (inproc, single process)\n\n";
    out << "Axes: nodes {1,4,16,64}, topics {1,4,16,64}, payload {8,4096} B; " << messages_per_pair << " messages per (publisher, topic) pair.\n";
    out << "Latency is one-way (steady_clock stamp in payload) over the in-process bus, "
        << "single executor. Footprint is the whole-process VmRSS/fd/thread sampled after "
        << "the cell's steady loop.\n";
    out << "\nPeak process VmRSS observed across the sweep: " << peak_rss_kib << " KiB.\n";
    out << "\nThe nodes=1 row carries no publisher peers (publishers live on nodes 1..N-1), so "
        << "its delivery latency is n/a by construction; it still exercises subscriber setup + "
        << "footprint.\n";
}

inline void emit_report(std::ostream &out, const sweep_axes &axes, const sweep_results &results)
{
    print_preamble(out, axes.messages_per_pair, results.peak_rss_kib);
    for(std::size_t pi = 0; pi < axes.payloads.size(); ++pi)
        print_matrix(out, axes, axes.payloads[pi], results.by_payload[pi]);
    const auto &heaviest = results.by_payload.back();
    print_setup_table(out, axes, heaviest);
    print_footprint_table(out, axes, "VmRSS", "KiB", heaviest, &footprint::rss_kib);
    print_footprint_table(out, axes, "open fds", "count", heaviest, &footprint::open_fds);
    print_footprint_table(out, axes, "threads", "count", heaviest, &footprint::threads);
}

}

#endif

// A self-contained, single-process scalability probe for plexus over the header-only in-process backend. It sweeps
// nodes x topics x payload and reports, per cell, the setup wall-time (construct all nodes + endpoints to readiness),
// the steady-state one-way delivery latency (p50 of a steady_clock stamp carried in the payload), and the process
// footprint (VmRSS, open fds, threads). It measures only plexus's own cost against its own axes.

#include "scale_cell.h"
#include "scale_report.h"

#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <algorithm>

namespace {

using namespace scale;

const std::array<std::size_t, 4> g_nodes    = {1, 4, 16, 64};
const std::array<std::size_t, 4> g_topics   = {1, 4, 16, 64};
const std::array<std::size_t, 2> g_payloads = {8, 4096};
constexpr std::uint64_t g_messages_per_pair = 200;

sweep_results run_sweep(const sweep_axes &axes)
{
    sweep_results results{.by_payload = std::vector<cell_grid>(axes.payloads.size())};
    for(std::size_t pi = 0; pi < axes.payloads.size(); ++pi)
        for(auto nodes : axes.nodes)
        {
            results.by_payload[pi].emplace_back();
            for(auto topics : axes.topics)
            {
                auto r               = run_cell(nodes, topics, axes.payloads[pi], axes.messages_per_pair);
                results.peak_rss_kib = std::max(results.peak_rss_kib, r.feet.rss_kib);
                results.by_payload[pi].back().push_back(r);
            }
        }
    return results;
}

}

int main(int argc, char **argv)
{
    const sweep_axes axes{.messages_per_pair = g_messages_per_pair, .nodes = g_nodes, .topics = g_topics, .payloads = g_payloads};
    const sweep_results results = run_sweep(axes);
    emit_report(std::cout, axes, results);

    const std::string path = argc > 1 ? argv[1] : "plexus_scale_report.md";
    std::ofstream file{path};
    if(file)
    {
        emit_report(file, axes, results);
        std::cerr << "\nreport written to " << path << '\n';
    }
    return 0;
}

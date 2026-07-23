#ifndef HPP_GUARD_BENCHMARKS_RELAY_RELAY_FORWARD_REPORT_H
#define HPP_GUARD_BENCHMARKS_RELAY_RELAY_FORWARD_REPORT_H

#include "relay_topology.h"

#include <array>
#include <vector>
#include <string>
#include <cstdio>
#include <ostream>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace relay_bench {

inline const std::array<std::size_t, 5> g_payloads = {8, 64, 1024, 4096, 16384};

struct cell
{
    double        per_msg_ns_med{};
    double        per_msg_ns_min{};
    double        throughput_mps{};
    std::uint64_t delivered{};
    std::uint64_t expected{};
};

inline double median(std::vector<double> &v)
{
    if(v.empty())
        return 0.0;
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

inline cell reduce(const std::vector<sample> &runs, std::uint64_t messages)
{
    cell c{};
    std::vector<double> per_msg;
    for(const auto &s : runs)
    {
        if(s.wall_ns == 0)
            continue;
        per_msg.push_back(static_cast<double>(s.wall_ns) / static_cast<double>(messages));
        c.delivered += s.delivered;
        c.expected += messages;
    }
    if(per_msg.empty())
        return c;
    c.per_msg_ns_min = *std::min_element(per_msg.begin(), per_msg.end());
    c.per_msg_ns_med = median(per_msg);
    c.throughput_mps = 1e9 / c.per_msg_ns_med;
    return c;
}

inline void emit(std::ostream &out, const std::vector<cell> &direct, const std::vector<cell> &relayed)
{
    out << "plexus relay forwarding — in-process (inproc) delivery cost, direct vs relayed\n";
    out << "single process, real steady_clock, one publish+drain per message; the relayed column adds\n";
    out << "one forward-splice hop (origin -> relay -> consumer, no direct origin<->consumer session).\n";
    out << "this is in-process forwarding overhead, NOT network latency, and NOT the on-target numerics.\n\n";
    out << "  payload   direct p50   relayed p50   hop overhead   overhead   direct thru   relayed thru\n";
    out << "   bytes         ns/msg        ns/msg         ns/msg          %          msg/s          msg/s\n";
    out << "  -------   ----------   -----------   ------------   --------   -----------   ------------\n";
    char line[256];
    for(std::size_t i = 0; i < g_payloads.size(); ++i)
    {
        const double d       = direct[i].per_msg_ns_med;
        const double r       = relayed[i].per_msg_ns_med;
        const double hop     = r - d;
        const double pct     = d > 0.0 ? 100.0 * hop / d : 0.0;
        std::snprintf(line, sizeof line, "  %7zu   %10.0f   %11.0f   %12.0f   %7.1f%%   %11.0f   %12.0f\n",
                      g_payloads[i], d, r, hop, pct, direct[i].throughput_mps, relayed[i].throughput_mps);
        out << line;
    }
    out << "\n";
    for(std::size_t i = 0; i < g_payloads.size(); ++i)
        if(direct[i].delivered != direct[i].expected || relayed[i].delivered != relayed[i].expected)
        {
            std::snprintf(line, sizeof line, "  WARNING payload %zu: delivered direct %llu/%llu relayed %llu/%llu\n", g_payloads[i],
                          static_cast<unsigned long long>(direct[i].delivered), static_cast<unsigned long long>(direct[i].expected),
                          static_cast<unsigned long long>(relayed[i].delivered), static_cast<unsigned long long>(relayed[i].expected));
            out << line;
        }
}

}

#endif

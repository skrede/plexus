#ifndef HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_TABLE_H
#define HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_TABLE_H

#include "self_loopback_report.h"

#include <array>
#include <string>
#include <vector>
#include <cstdio>
#include <cstddef>
#include <ostream>
#include <algorithm>

namespace self_loopback {

// Per-carrier results: for each payload, the per-run cells (one entry per repeat). A carrier
// that did not run on this host (e.g. SHM off-gate) leaves its rows empty.
struct lane_runs
{
    std::string                     name;
    std::array<std::vector<cell>, 4> by_payload{};
    bool                            ran{};
};

inline double median_of(std::vector<double> v)
{
    if(v.empty())
        return 0.0;
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

inline std::string fmt(double v)
{
    char buf[32];
    std::snprintf(buf, sizeof buf, "%.3f", v);
    return buf;
}

inline std::vector<double> p50s(const std::vector<cell> &runs)
{
    std::vector<double> out;
    for(const auto &c : runs)
        if(c.ran)
            out.push_back(c.p50_us);
    return out;
}

inline std::vector<double> tputs(const std::vector<cell> &runs)
{
    std::vector<double> out;
    for(const auto &c : runs)
        if(c.ran)
            out.push_back(c.throughput_mps);
    return out;
}

inline void emit_header(std::ostream &out, const char *what)
{
    out << '\n' << what << "\n\n| carrier |";
    for(auto p : g_payloads)
        out << ' ' << p << " B |";
    out << "\n|---|";
    for(std::size_t i = 0; i < g_payloads.size(); ++i)
        out << "---|";
    out << '\n';
}

// The column-winning lane per payload by median p50 (lower is faster); -1 when no lane ran.
inline std::array<int, 4> fastest_per_payload(const std::vector<lane_runs> &lanes)
{
    std::array<int, 4> winner;
    for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
    {
        int    best = -1;
        double low  = 1.0e300;
        for(std::size_t li = 0; li < lanes.size(); ++li)
        {
            const auto vs = p50s(lanes[li].by_payload[pi]);
            if(vs.empty())
                continue;
            const double m = median_of(vs);
            if(m < low)
            {
                low  = m;
                best = static_cast<int>(li);
            }
        }
        winner[pi] = best;
    }
    return winner;
}

inline void emit_cell(std::ostream &out, std::vector<double> vals, bool bold)
{
    if(vals.empty())
    {
        out << " n/a |";
        return;
    }
    const std::string s = fmt(median_of(std::move(vals)));
    out << ' ' << (bold ? "**" + s + "**" : s) << " |";
}

inline void emit_headline(std::ostream &out, const std::vector<lane_runs> &lanes)
{
    const auto win = fastest_per_payload(lanes);
    emit_header(out, "### head-to-head: median p50 one-way latency (us), fastest **bold**");
    for(std::size_t li = 0; li < lanes.size(); ++li)
    {
        out << "| " << lanes[li].name << " |";
        for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
            emit_cell(out, p50s(lanes[li].by_payload[pi]), win[pi] == static_cast<int>(li));
        out << '\n';
    }
}

// The drive-bound message rate is non-diagnostic when the lane is pumped per message (its
// wall-time is reactor scheduling, not delivery); print n/a rather than a misleading near-zero.
inline void emit_tput(std::ostream &out, std::vector<double> vals)
{
    if(vals.empty())
    {
        out << " n/a |";
        return;
    }
    const double m = median_of(std::move(vals));
    out << (m < 0.01 ? std::string(" n/a |") : " " + fmt(m) + " |");
}

// One lane's spread table: median p50, the min/max p50 across runs, and the drive-bound rate,
// so a reader sees whether the spread is noise (feedback_no_success_from_single_run).
inline void emit_lane(std::ostream &out, const lane_runs &lane, std::size_t run_count)
{
    out << "\n### carrier `" << lane.name << "` -- " << run_count << " runs (p50 us, [min..max], drive-bound Mmsg/s)\n\n";
    out << "| payload | p50 (med) | p50 min | p50 max | drive-bound rate |\n|---|---|---|---|---|\n";
    for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
    {
        auto vs = p50s(lane.by_payload[pi]);
        out << "| " << g_payloads[pi] << " B |";
        emit_cell(out, vs, false);
        out << (vs.empty() ? " n/a |" : " " + fmt(*std::min_element(vs.begin(), vs.end())) + " |");
        out << (vs.empty() ? " n/a |" : " " + fmt(*std::max_element(vs.begin(), vs.end())) + " |");
        emit_tput(out, tputs(lane.by_payload[pi]));
        out << '\n';
    }
}

}

#endif

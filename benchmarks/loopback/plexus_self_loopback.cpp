// A single-process cross-transport same-node self-delivery probe. It measures the two carriers
// a node uses to deliver a publish back to a subscriber on the SAME node, head-to-head over a
// payload sweep: the intra-node self-route (a single-transport loopback node — a typed object
// lane that delivers by address with zero serialization, plus its framed bytes lane) and the
// shm self-ring fallback (a node with an shm member but no intra-node transport). Each point is
// repeated so the report carries min/median/spread, never a single run.

#include "self_loopback_table.h"
#include "self_loopback_report.h"
#include "self_loopback_lane_shm.h"
#include "self_loopback_lane_intra.h"

#include <vector>
#include <cstdint>
#include <fstream>
#include <iostream>

namespace {

using namespace self_loopback;

constexpr std::uint64_t g_messages = 2000;
constexpr std::size_t   g_runs     = 5;

template<typename Point>
lane_runs sweep(const char *name, Point point)
{
    lane_runs lane{name};
    for(std::size_t pi = 0; pi < g_payloads.size(); ++pi)
        for(std::size_t r = 0; r < g_runs; ++r)
        {
            auto c = point(g_payloads[pi], g_messages);
            lane.ran = lane.ran || c.ran;
            lane.by_payload[pi].push_back(c);
        }
    return lane;
}

lane_runs shm_lane()
{
#ifdef PLEXUS_ENABLE_SHM_BACKEND
    return sweep("shm-self (bytes)", shm_bytes_point);
#else
    lane_runs lane{"shm-self (bytes)"};
    return lane;
#endif
}

void emit_report(std::ostream &out, const std::vector<lane_runs> &lanes)
{
    out << "# plexus cross-transport same-node self-delivery (single process)\n\n";
    out << "Payloads {8,64,1024,4096} B; " << g_messages << " messages per point; " << g_runs << " runs per point.\n";
    out << "Latency is one-way (a steady_clock stamp carried in the payload/object). The per-carrier\n"
        << "rate is drive-bound (each point is pumped one message at a time so a one-way delta is\n"
        << "captured per publish); it is not a saturation throughput and is non-diagnostic where the\n"
        << "carrier's wall-time is reactor scheduling (shown n/a).\n";
    emit_headline(out, lanes);
    for(const auto &lane : lanes)
        emit_lane(out, lane, g_runs);
    out << "\nThe intra-node typed lane delivered with encodes==0 (asserted in-run); the shm-self lane "
        << (lanes.back().ran ? "ran on this host." : "was SKIPPED (shm backend off / not present).") << '\n';
}

}

int main(int argc, char **argv)
{
    std::vector<lane_runs> lanes;
    lanes.push_back(sweep("intra-node (typed, zero-copy)", intra_typed_point));
    lanes.push_back(sweep("intra-node (bytes)", intra_bytes_point));
    lanes.push_back(shm_lane());

    emit_report(std::cout, lanes);

    const std::string path = argc > 1 ? argv[1] : "plexus_self_loopback_report.md";
    std::ofstream     file{path};
    if(file)
    {
        emit_report(file, lanes);
        std::cerr << "\nreport written to " << path << '\n';
    }
    return 0;
}

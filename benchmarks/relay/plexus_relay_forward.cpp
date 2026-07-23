// A single-process probe of the cost of relayed forwarding against a direct baseline. Three inproc
// engines (origin -> relay -> consumer, the consumer reaching the origin ONLY through the relay's
// forward_splice) are compared head-to-head with a two-engine direct pair over a payload sweep. Each
// point publishes and drains one message at a time on the real steady_clock and is repeated, so the
// report carries the median and the relayed-minus-direct hop overhead, never a single run.

#include "relay_forward_report.h"
#include "relay_topology.h"

#include <vector>
#include <fstream>
#include <iostream>

namespace {

using namespace relay_bench;

constexpr std::uint64_t g_messages = 2000;
constexpr std::size_t   g_runs     = 5;

template<typename Line>
cell sweep_point(std::size_t payload)
{
    std::vector<sample> runs;
    for(std::size_t r = 0; r < g_runs; ++r)
    {
        Line line;
        line.connect("bench/relay/telemetry");
        runs.push_back(time_publishes(line, "bench/relay/telemetry", payload, g_messages));
    }
    return reduce(runs, g_messages);
}

}

int main(int argc, char **argv)
{
    std::vector<cell> direct, relayed;
    for(std::size_t payload : g_payloads)
    {
        direct.push_back(sweep_point<direct_line>(payload));
        relayed.push_back(sweep_point<relay_line>(payload));
    }

    emit(std::cout, direct, relayed);

    if(argc > 1)
    {
        std::ofstream file(argv[1]);
        emit(file, direct, relayed);
        std::cerr << "wrote " << argv[1] << "\n";
    }
    return 0;
}

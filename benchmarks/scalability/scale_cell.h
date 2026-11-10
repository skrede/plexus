#ifndef HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_CELL_H
#define HPP_GUARD_BENCHMARKS_SCALABILITY_SCALE_CELL_H

#include "scale_footprint.h"

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
#include <deque>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace scale {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using clock_type = std::chrono::steady_clock;
using node_type  = plexus::node<inproc_policy, inproc_transport<>>;

struct latency
{
    double p50_us{};
    double p99_us{};
    std::uint64_t delivered{};
};

inline double percentile(const std::vector<double> &sorted, double q)
{
    if(sorted.empty())
        return 0.0;
    const auto rank = static_cast<std::size_t>(q * static_cast<double>(sorted.size() - 1));
    return sorted[rank];
}

inline latency reduce(std::vector<double> &samples_us)
{
    std::sort(samples_us.begin(), samples_us.end());
    return {percentile(samples_us, 0.50), percentile(samples_us, 0.99), static_cast<std::uint64_t>(samples_us.size())};
}

inline plexus::node_options publisher_opts(std::uint64_t seed)
{
    plexus::node_options o;
    o.redial_seed  = seed;
    o.dial_eagerly = false;
    return o;
}

inline plexus::node_options subscriber_opts()
{
    plexus::node_options o;
    o.redial_seed  = 0x5;
    o.dial_eagerly = true;
    return o;
}

// One cell's live graph: node 0 is the single eager dialer and holds one subscriber per topic; the other nodes are
// lazy publishers, one publisher per topic, that it dials once the shared discovery table makes them known. Every
// subscriber receives from every publisher on its topic, so the matched (node, topic) fan-in is what scales. Nodes
// and transports are non-movable, so they live in deques whose element addresses never shift as the graph grows;
// the transports are declared before the nodes so the nodes that use them are destroyed first.
struct mesh
{
    inproc_bus<> bus;
    inproc_executor<> ex;
    static_discovery disc;
    std::deque<inproc_transport<>> transports;
    std::deque<node_type> nodes;

    explicit mesh(std::size_t node_count)
            : ex{bus}
            , disc{{}}
    {
        for(std::size_t i = 0; i < node_count; ++i)
        {
            auto &t                  = transports.emplace_back(ex, bus);
            const bool is_subscriber = i == 0;
            auto opts                = is_subscriber ? subscriber_opts() : publisher_opts(0x100 + i);
            nodes.emplace_back(ex, disc, "scale-node-" + std::to_string(i), t, opts);
        }
        for(std::size_t i = 0; i < node_count; ++i)
            nodes[i].listen({"inproc", "host-" + std::to_string(i) + ":" + std::to_string(5000 + i)});
        ex.drain();
    }
};

inline std::string topic_name(std::size_t t)
{
    return "scale/topic/" + std::to_string(t);
}

inline void write_stamp(std::span<std::byte> payload)
{
    const auto now = static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
    for(std::size_t i = 0; i < sizeof now; ++i)
        payload[i] = static_cast<std::byte>((now >> (8 * i)) & 0xff);
}

inline std::uint64_t read_stamp(std::span<const std::byte> bytes)
{
    std::uint64_t v = 0;
    for(std::size_t i = 0; i < sizeof v; ++i)
        v |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[i])) << (8 * i);
    return v;
}

struct cell_result
{
    latency lat;
    footprint feet;
    double setup_ms{};
};

inline std::deque<plexus::subscriber<>> subscribe(mesh &m, std::size_t topic_count, std::vector<double> &samples_us)
{
    std::deque<plexus::subscriber<>> subs;
    for(std::size_t t = 0; t < topic_count; ++t)
        subs.emplace_back(m.nodes[0], topic_name(t),
                          [&samples_us](std::span<const std::byte> bytes)
                          {
                              const auto recv     = static_cast<std::uint64_t>(clock_type::now().time_since_epoch().count());
                              const auto delta_ns = static_cast<double>(recv - read_stamp(bytes));
                              samples_us.push_back(delta_ns / 1000.0);
                          });
    return subs;
}

inline std::deque<plexus::publisher<>> advertise(mesh &m, std::size_t topic_count)
{
    std::deque<plexus::publisher<>> pubs;
    for(std::size_t n = 1; n < m.nodes.size(); ++n)
        for(std::size_t t = 0; t < topic_count; ++t)
            pubs.emplace_back(m.nodes[n], topic_name(t));
    return pubs;
}

inline void drive(mesh &m, std::deque<plexus::publisher<>> &pubs, std::span<std::byte> payload, std::uint64_t messages_per_pair)
{
    for(std::uint64_t i = 0; i < messages_per_pair; ++i)
        for(auto &p : pubs)
        {
            write_stamp(payload);
            p.publish(std::span<const std::byte>{payload});
            m.ex.drain();
        }
}

inline cell_result run_cell(std::size_t node_count, std::size_t topic_count, std::size_t payload_bytes, std::uint64_t messages_per_pair)
{
    std::vector<double> samples_us;

    const auto t_setup0 = clock_type::now();
    mesh m{node_count};
    auto subs = subscribe(m, topic_count, samples_us);
    auto pubs = advertise(m, topic_count);
    m.ex.drain();
    const double setup_ms = std::chrono::duration<double, std::milli>(clock_type::now() - t_setup0).count();

    std::vector<std::byte> payload(std::max<std::size_t>(payload_bytes, sizeof(std::uint64_t)));
    drive(m, pubs, payload, messages_per_pair);
    return {.lat = reduce(samples_us), .feet = sample_footprint(), .setup_ms = setup_ms};
}

}

#endif

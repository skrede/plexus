#ifndef HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_LANE_SHM_H
#define HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_LANE_SHM_H

#include "self_loopback_report.h"

#ifdef PLEXUS_ENABLE_SHM_BACKEND

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/shm/linux/shm_member.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include <asio/io_context.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

#include <unistd.h>

namespace self_loopback {

namespace pasio = plexus::asio;

// A node with an shm member and NO intra-node transport: a same-node publish must fall through
// to the node-local shm self-ring (the fallback carrier this lane measures).
using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

inline plexus::node_id shm_id()
{
    plexus::node_id id{};
    id[0] = std::byte{0x71};
    return id;
}

inline std::string shm_region_ns(std::size_t payload)
{
    return "bench-self-" + std::to_string(::getpid()) + "-" + std::to_string(payload);
}

// Pump the reactor until the predicate holds or the deadline elapses (the ring's wake rides an
// io_uring futex-wait → eventfd → asio turn, so a bounded run_for delivers without spinning).
template<typename Pred>
void pump_until(::asio::io_context &io, Pred done, std::chrono::milliseconds budget = std::chrono::milliseconds{500})
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(!done() && std::chrono::steady_clock::now() < deadline)
    {
        io.restart();
        io.run_for(std::chrono::milliseconds{5});
    }
}

inline cell shm_bytes_point(std::size_t payload, std::uint64_t messages)
{
    ::asio::io_context                      io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery     disc{{}};
    pasio::shm::shm_member shm{pasio::shm::make_shm_member(io, broker, plexus::io::reliability::reliable, plexus::io::congestion::block, shm_region_ns(payload))};
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    shm_node              node{io, disc, shm_id(), shm, local, remote, plexus::node_options{}};

    std::vector<double>  samples;
    plexus::subscriber<> sub{node, "bench/shm", [&](std::span<const std::byte> b)
                             { samples.push_back(static_cast<double>(now_count() - read_stamp(b)) / 1000.0); }};
    plexus::publisher<>  pub{node, "bench/shm"};
    pump_until(io, [] { return false; }, std::chrono::milliseconds{50});

    std::vector<std::byte> frame(std::max<std::size_t>(payload, sizeof(std::uint64_t)));
    const auto             t0 = clock_type::now();
    for(std::uint64_t i = 0; i < messages; ++i)
    {
        const auto target = samples.size() + 1;
        write_stamp(frame, now_count());
        pub.publish(std::span<const std::byte>{frame});
        pump_until(io, [&] { return samples.size() >= target; });
    }
    return reduce(samples, std::chrono::duration<double>(clock_type::now() - t0).count());
}

}

#endif

#endif

#ifndef HPP_GUARD_TESTS_NODE_TEST_SHM_SELF_CARRIER_COMMON_H
#define HPP_GUARD_TESTS_NODE_TEST_SHM_SELF_CARRIER_COMMON_H

#include "plexus/asio/detail/same_host_shm_config.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/platform.h"

#include <asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <string>
#include <cstddef>
#include <functional>

namespace plexus_test {

namespace pasio = plexus::asio;

// A node with NO intra_node transport but WITH an shm member (shm + unix + tcp). The intra-node
// self-route is therefore ABSENT; a same-node publish→subscribe must fall through to the shm
// self-ring (the fallback carrier this slice proves).
using shm_self_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport>;

inline plexus::node_id make_id(std::byte first) noexcept
{
    plexus::node_id id{};
    id[0] = first;
    return id;
}

// A region namespace unique per fixture instance (and per process), so each test case's self-ring is
// isolated — two cases sharing the by-topic-name default would otherwise join the SAME live ring and
// read each other's stale frames (the deterministic-name co-host sharing model the namespace exists
// to partition).
inline std::string unique_region_ns()
{
    static std::atomic<unsigned> seq{0};
    return "self-carrier-" + std::to_string(plexus::testing::process_id()) + "-" + std::to_string(seq.fetch_add(1));
}

// Owns the io substrate, broker, and the borrowed leaves so the node outlives nothing it borrows.
struct shm_fixture
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};
    pasio::shm::shm_member shm{pasio::shm::make_shm_member(io, broker, plexus::io::reliability::reliable, plexus::io::congestion::block, unique_region_ns())};
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    shm_self_node node{io, disc, make_id(std::byte{0x71}), shm, local, remote, plexus::node_options{}};

    // Drive the reactor until the predicate holds or the deadline elapses (the shm ring's wake rides
    // an io_uring futex-wait → eventfd → asio turn, so a bounded run_for delivers without spinning).
    template<typename Pred>
    void drive_until(Pred done, std::chrono::milliseconds budget = std::chrono::milliseconds{500})
    {
        const auto deadline = std::chrono::steady_clock::now() + budget;
        while(!done() && std::chrono::steady_clock::now() < deadline)
        {
            io.restart();
            io.run_for(std::chrono::milliseconds{20});
        }
    }
};

}

#endif

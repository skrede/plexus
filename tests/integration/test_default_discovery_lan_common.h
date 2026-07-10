#ifndef HPP_GUARD_TESTS_INTEGRATION_DEFAULT_DISCOVERY_LAN_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_DEFAULT_DISCOVERY_LAN_COMMON_H

#include "plexus/asio/default_discovery.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"

#include "plexus/node_id.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace pasio = plexus::asio;
using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;
using plexus::discovery::discovery_health;

namespace default_discovery_lan_fixture {

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0x5a)};
    return id;
}

// redial_seed is required-distinct; awareness here is lazy (dial_eagerly defaults false), so a
// noted peer is recorded in known() without a dial — the value is inert for this awareness proof.
inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

template<typename Pred>
bool pump_until(::asio::io_context &io, Pred pred, std::chrono::milliseconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
    return pred();
}

inline std::int64_t median_ms(std::vector<std::int64_t> xs)
{
    std::sort(xs.begin(), xs.end());
    return xs.empty() ? 0 : xs[xs.size() / 2];
}

}

#endif

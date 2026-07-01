#ifndef HPP_GUARD_PLEXUS_EXAMPLE_BENCH_NODE_H
#define HPP_GUARD_PLEXUS_EXAMPLE_BENCH_NODE_H

#include "bench_workload.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/freertos_executor.h"

#include <chrono>
#include <memory>
#include <optional>

namespace example {

// The shared bench-node prologue every workload builds: a node over a constructed transport (borrowing
// disc + the transport by reference, so both must outlive it — disc lives in the caller's frame) dialed
// at the peer. Factored so each drive() body is just its workload-specific publishers/subscribers.
template<typename Policy, typename Transport>
std::unique_ptr<plexus::node<Policy, Transport>> dial_bench_node(plexus::discovery::static_discovery &disc, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::node_options opts;
    opts.name              = "esp32-lwip-bench";
    opts.max_message_bytes = k_max_tier_bytes;
    opts.reconnect         = plexus::io::reconnect_config{std::chrono::milliseconds{200}, std::chrono::seconds{5}, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x1F1C0DE;
    auto node = std::make_unique<plexus::node<Policy, Transport>>(ex, disc, opts.name, transport, opts);
    node->dial({scheme, endpoint});
    return node;
}

}

#endif

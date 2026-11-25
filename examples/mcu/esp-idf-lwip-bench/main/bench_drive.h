#ifndef HPP_GUARD_EXAMPLES_MCU_ESP_IDF_LWIP_BENCH_MAIN_BENCH_DRIVE_H
#define HPP_GUARD_EXAMPLES_MCU_ESP_IDF_LWIP_BENCH_MAIN_BENCH_DRIVE_H

#include "bench_node.h"
#include "bench_oneway.h"
#include "bench_runner.h"
#include "bench_pipeline.h"
#include "bench_workload.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/message_info.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include <span>
#include <cstddef>

namespace example {

// One workload over a constructed transport: the executor, transport, and node all live on this
// task's stack and the node borrows the first two by reference, so they outlive it; run() never
// returns. dial uses the transport's own scheme so the serial and lwIP cells share this body.
template<typename Policy, typename Transport>
void drive(const char *policy_name, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};
    auto node = dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> request{*node, "request"};
    echo_probe probe{request, k_payload_tiers[0]};
    bench_runner runner{policy_name, probe};

    plexus::subscriber<void> reply{*node, "reply", [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   { runner.on_reply(bytes); }};

    plexus::freertos::freertos_timer kick{ex};
    resend_pump pump{kick, probe};

    runner.start();
    pump.arm();
    plexus::freertos::run(ex, transport);
}

// The one-way streaming workload: the device saturates the "stream" topic under drop_newest and the
// host measures the delivered rate; the device reports accepted = offered - dropped() per tier. The
// paced timer re-arms so the run loop drains egress between offers (never a tight publish loop).
template<typename Policy, typename Transport>
void drive_oneway(const char *policy_name, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};
    auto node = dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> stream{*node, "stream"};
    oneway_driver driver{stream, k_payload_tiers[0]};
    oneway_runner runner{policy_name, driver};
    oneway_source<Transport> source{runner, transport};

    runner.start();
    plexus::freertos::run(ex, transport, source);
}

// The pipelined request/echo workload: the device holds `window` round trips in flight (a ring of that many
// tagged slots) under congestion=block, refilling one on each matching reply. A window of 1 reduces to the
// single-in-flight latency path; a coarse pump re-issues a genuinely lost slot so the window never wedges.
template<typename Policy, typename Transport>
void drive_pipeline(const char *policy_name, std::size_t window, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};
    auto node = dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> request{*node, "request"};
    windowed_probe probe{policy_name, request, window, k_payload_tiers[0]};

    plexus::subscriber<void> reply{*node, "reply", [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   { probe.on_reply(bytes); }};

    plexus::freertos::freertos_timer kick{ex};
    pipeline_pump pump{kick, probe};

    probe.start();
    pump.arm();
    plexus::freertos::run(ex, transport);
}

}

#endif

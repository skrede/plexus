// The on-device bench firmware: a request/echo round-trip workload at fixed payload tiers over one
// of three transports (serial, lwIP-P1 poll-drive, lwIP-P2 RX task), selected at build time. For
// each tier the device publishes a tier-sized request stamped with the send time and awaits the
// host's echo on the reply topic, recording the round-trip microseconds; it also samples the RX
// task's stack high-water and the free heap so P2's RAM/stack cost is measured, not estimated. Each
// sample is emitted as a machine-parseable line on the console (UART0); the runner parses them.
//
// The same workload runs over all three transports — only the transport+policy and the receive
// policy (P1 poll vs P2 RX task) differ per build. The console stays on UART0 for the sample lines;
// the serial cell's plexus link rides UART1 (bench_uart.h) so the two never share a port.

#include "bench_uart.h"
#include "bench_node.h"
#include "wifi_netif.h"
#include "bench_oneway.h"
#include "bench_runner.h"
#include "bench_pipeline.h"
#include "bench_workload.h"

#include "plexus/io/endpoint.h"

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/lwip_socket_io.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <span>
#include <memory>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <string_view>
#include <system_error>

// The build-time transport selector: BENCH_SERIAL, BENCH_LWIP_P1, or BENCH_LWIP_P2. Default to the
// lwIP-P2 cell so a plain build is a valid runnable; the runner sets exactly one per cell.
#if !defined(BENCH_SERIAL) && !defined(BENCH_LWIP_P1) && !defined(BENCH_LWIP_P2)
    #define BENCH_LWIP_P2
#endif

namespace {

using lwip_socket    = plexus::freertos::detail::lwip_socket;
using lwip_policy    = plexus::freertos::lwip_policy<lwip_socket>;
using lwip_transport = plexus::freertos::lwip_transport<lwip_socket>;

#ifndef PLEXUS_HOST_ENDPOINT
    #define PLEXUS_HOST_ENDPOINT "192.168.1.69:7447"
#endif
constexpr const char *k_host_endpoint = PLEXUS_HOST_ENDPOINT;

// The pipeline window depth is a build define so the K-sweep rebuilds one binary per K; it rides the
// policy label ("lwip-p2-k<K>") so every emitted line carries the depth. PP_STR stringizes the K value.
#ifndef BENCH_WINDOW_K
    #define BENCH_WINDOW_K 1
#endif
#define PP_STR2(x) #x
#define PP_STR(x)  PP_STR2(x)

constexpr std::uint32_t k_plexus_task_stack = 12288; // bytes (xTaskCreate takes bytes)
constexpr UBaseType_t   k_plexus_task_prio  = 5;
constexpr std::uint32_t k_rx_task_stack     = 4096;  // bytes; the RX task parks in a blocking recv

// The deep egress cap for the widen-cost variant. Empirical, heap-bounded: the sweep emits free heap so
// the value is confirmed to fit alongside the Wi-Fi stack, not guessed. The 8 KiB default is the headline.
constexpr std::size_t k_deep_egress_bytes = 64 * 1024;

// The pipeline primes k_max_window requests of the largest tier before the run loop drains, so the egress
// cap must exceed that framed sum (16 x 4096 B + framing) or a deep window blocks on the cap. Twice the
// sum gives framing margin; it is a soft limit (Slice 2: the send queue is not pre-allocated) sized here
// for the application's pipeline depth, not a change to the plexus default.
constexpr std::size_t k_pipeline_egress_bytes = 2 * example::k_max_window * example::k_max_tier_bytes;

// One workload over a constructed transport: the executor, transport, and node all live on this
// task's stack and the node borrows the first two by reference, so they outlive it; run() never
// returns. dial uses the transport's own scheme so the serial and lwIP cells share this body.
template<typename Policy, typename Transport>
void drive(const char *policy_name, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};
    auto node = example::dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> request{*node, "request"};
    example::echo_probe probe{request, example::k_payload_tiers[0]};
    example::bench_runner runner{policy_name, probe};

    plexus::subscriber<void> reply{*node, "reply", [&](std::span<const std::byte> bytes, const plexus::io::message_info &) { runner.on_reply(bytes); }};

    plexus::freertos::freertos_timer kick{ex};
    example::resend_pump             pump{kick, probe};

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
    auto node = example::dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> stream{*node, "stream"};
    example::oneway_driver  driver{stream, example::k_payload_tiers[0]};
    example::oneway_runner  runner{policy_name, driver};
    example::oneway_source<Transport> source{runner, transport};

    runner.start();
    plexus::freertos::run(ex, transport, source);
}

// The pipelined request/echo workload: the device holds BENCH_WINDOW_K round trips in flight (a ring of K
// tagged slots) under congestion=block, refilling one on each matching reply. K=1 reduces to the
// single-in-flight latency path; a coarse pump re-issues a genuinely lost slot so the window never wedges.
template<typename Policy, typename Transport>
void drive_pipeline(const char *policy_name, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};
    auto node = example::dial_bench_node<Policy, Transport>(disc, transport, ex, scheme, endpoint);

    plexus::publisher<void> request{*node, "request"};
    example::windowed_probe probe{policy_name, request, BENCH_WINDOW_K, example::k_payload_tiers[0]};

    plexus::subscriber<void> reply{*node, "reply", [&](std::span<const std::byte> bytes, const plexus::io::message_info &) { probe.on_reply(bytes); }};

    plexus::freertos::freertos_timer kick{ex};
    example::pipeline_pump           pump{kick, probe};

    probe.start();
    pump.arm();
    plexus::freertos::run(ex, transport);
}

#if defined(BENCH_SERIAL)
void plexus_task(void *)
{
    plexus::freertos::freertos_executor ex;
    example::bench_uart_transport transport;
    #if defined(BENCH_WORKLOAD_ONEWAY)
    drive_oneway<example::bench_uart_policy>("serial-b" PP_STR(BENCH_BAUD), transport, ex, "serial", "uart1");
    #elif defined(BENCH_WORKLOAD_PIPELINE)
    constexpr const char *serial_pipeline_policy = "serial-k" PP_STR(BENCH_WINDOW_K);
    drive_pipeline<example::bench_uart_policy>(serial_pipeline_policy, transport, ex, "serial", "uart1");
    #else
    drive<example::bench_uart_policy>("serial", transport, ex, "serial", "uart1");
    #endif
}
#else
void plexus_task(void *)
{
    if(!example::wifi_connect_sta())
        return;
    plexus::freertos::freertos_executor ex;
    #if defined(BENCH_WORKLOAD_ONEWAY)
    // The one-way stream is device->host only: no ingress, so the poll-drive (P1) receive policy fits and
    // avoids an idle RX task; transport.poll() drains egress each run-loop tick, completing large frames.
    using lim = plexus::freertos::lwip_channel_limits;
    #if defined(BENCH_DEEP_EGRESS)
    constexpr std::size_t oneway_egress_cap = k_deep_egress_bytes;
    constexpr const char *oneway_policy     = "lwip-p1-deep";
    #else
    constexpr std::size_t oneway_egress_cap = lim::egress_cap_bytes;
    constexpr const char *oneway_policy     = "lwip-p1";
    #endif
    lwip_transport transport{ex, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, oneway_egress_cap, plexus::io::congestion::drop_newest};
    drive_oneway<lwip_policy>(oneway_policy, transport, ex, "tcp", k_host_endpoint);
    #elif defined(BENCH_WORKLOAD_PIPELINE)
    // Request/reply has ingress, so the P2 RX task drives receipt; congestion=block never sheds a request
    // (a shed request would silently vanish and wedge a slot). The egress cap holds the primed window.
    using lim = plexus::freertos::lwip_channel_limits;
    constexpr const char *pipeline_policy = "lwip-p2-k" PP_STR(BENCH_WINDOW_K);
    lwip_transport transport{ex, lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, k_pipeline_egress_bytes, plexus::io::congestion::block};
    transport.use_rx_task(plexus::freertos::task_options{k_rx_task_stack});
    drive_pipeline<lwip_policy>(pipeline_policy, transport, ex, "tcp", k_host_endpoint);
    #else
    lwip_transport transport{ex};
    #if defined(BENCH_LWIP_P2)
    transport.use_rx_task(plexus::freertos::task_options{k_rx_task_stack});
    drive<lwip_policy>("lwip-p2", transport, ex, "tcp", k_host_endpoint);
    #else
    drive<lwip_policy>("lwip-p1", transport, ex, "tcp", k_host_endpoint);
    #endif
    #endif
}
#endif

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}

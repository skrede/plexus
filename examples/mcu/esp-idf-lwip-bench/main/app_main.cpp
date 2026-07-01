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
#include "wifi_netif.h"
#include "bench_runner.h"
#include "bench_workload.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

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

constexpr std::uint32_t k_plexus_task_stack = 12288; // bytes (xTaskCreate takes bytes)
constexpr UBaseType_t   k_plexus_task_prio  = 5;
constexpr std::uint32_t k_rx_task_stack     = 4096;  // bytes; the RX task parks in a blocking recv

// One workload over a constructed transport: the executor, transport, and node all live on this
// task's stack and the node borrows the first two by reference, so they outlive it; run() never
// returns. dial uses the transport's own scheme so the serial and lwIP cells share this body.
template<typename Policy, typename Transport>
[[noreturn]] void drive(const char *policy_name, Transport &transport, plexus::freertos::freertos_executor &ex, const char *scheme, const char *endpoint)
{
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name              = "esp32-lwip-bench";
    opts.max_message_bytes = example::k_max_tier_bytes;
    opts.reconnect         = plexus::io::reconnect_config{std::chrono::milliseconds{200}, std::chrono::seconds{5}, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x1F1C0DE;

    auto node = std::make_unique<plexus::node<Policy, Transport>>(ex, disc, opts.name, transport, opts);
    node->dial({scheme, endpoint});

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

#if defined(BENCH_SERIAL)
void plexus_task(void *)
{
    plexus::freertos::freertos_executor ex;
    example::bench_uart_transport transport;
    drive<example::bench_uart_policy>("serial", transport, ex, "serial", "uart1");
}
#else
void plexus_task(void *)
{
    if(!example::wifi_connect_sta())
        return;
    plexus::freertos::freertos_executor ex;
    lwip_transport transport{ex};
    #if defined(BENCH_LWIP_P2)
    transport.use_rx_task(plexus::freertos::task_options{k_rx_task_stack});
    drive<lwip_policy>("lwip-p2", transport, ex, "tcp", k_host_endpoint);
    #else
    drive<lwip_policy>("lwip-p1", transport, ex, "tcp", k_host_endpoint);
    #endif
}
#endif

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}

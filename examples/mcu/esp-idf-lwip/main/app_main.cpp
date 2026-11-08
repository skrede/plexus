#include "wifi_netif.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_rx_task.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/detail/null_acceptor.h"
#include "plexus/freertos/detail/lwip_socket_io.h"
#include "plexus/freertos/detail/lwip_acceptor_io.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_log.h"

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <cstdint>
#include <cstddef>
#include <optional>
#include <cinttypes>
#include <system_error>

namespace {

#ifndef PLEXUS_PUBLISH_PERIOD_MS
    #define PLEXUS_PUBLISH_PERIOD_MS 1000
#endif

// A dial to a dead endpoint holds a pending slot until its dial timeout, so it needs one beside the host dial's.
#ifdef PLEXUS_DEAD_ENDPOINT
constexpr std::size_t k_max_clients = 2;
#else
constexpr std::size_t k_max_clients = 1;
#endif

using lwip_socket    = plexus::freertos::detail::lwip_socket;
using lwip_acceptor  = plexus::freertos::detail::lwip_acceptor;
using lwip_policy    = plexus::freertos::lwip_policy<lwip_socket>;
using lwip_transport = plexus::freertos::lwip_transport<lwip_socket, plexus::freertos::detail::null_acceptor<lwip_socket>, k_max_clients>;

// This dialing example uses the dial-only transport (the default null_acceptor); naming the server
// transport AND driving its listen/poll forces the ESP-gated acceptor leaf (bind/listen/accept) and
// the bounded-view machinery through the cross-compiler so the server path is link-proven on-target,
// not only host-green. The function below is never called — it exists for the instantiation only.
using lwip_server_transport = plexus::freertos::lwip_transport<lwip_socket, lwip_acceptor>;
static_assert(plexus::io::transport_backend<lwip_server_transport, lwip_policy>,
              "the lwIP listen/accept transport must satisfy transport_backend on-target");

[[maybe_unused]] void instantiate_lwip_server_path(plexus::freertos::freertos_executor &ex)
{
    lwip_server_transport server{ex};
    server.listen({"tcp", "0.0.0.0:7447"});
    server.poll();
    server.close();
}

#ifndef PLEXUS_HOST_ENDPOINT
    #define PLEXUS_HOST_ENDPOINT "192.168.1.69:7447"
#endif
constexpr const char *k_host_endpoint = PLEXUS_HOST_ENDPOINT;

constexpr const char *k_log_tag = "plexus-liveness";
constexpr std::chrono::milliseconds k_publish_period{PLEXUS_PUBLISH_PERIOD_MS};
constexpr std::size_t k_max_payload_bytes   = 256;
constexpr std::uint32_t k_plexus_task_stack = 16384; // bytes (ESP-IDF xTaskCreate takes bytes)
constexpr UBaseType_t k_plexus_task_prio    = 5;
constexpr std::uint32_t k_rx_task_stack     = 4096; // bytes

// A monotonic counter byte stands in for a real sensor reading — a deterministic payload for the
// multi-run host gate, no GPIO needed.
struct telemetry_source
{
    std::uint8_t counter{0};

    std::array<std::byte, 1> next()
    {
        return {static_cast<std::byte>(counter++)};
    }
};

struct publish_loop
{
    plexus::freertos::freertos_timer &timer;
    plexus::publisher<void> &topic;
    telemetry_source &source;

    void arm()
    {
        timer.expires_after(k_publish_period);
        timer.async_wait(
            [this](std::error_code)
            {
                const auto reading = source.next();
                topic.publish(std::span<const std::byte>{reading});
                arm();
            });
    }
};

// The smoke reads executor liveness from the gaps between these lines' log timestamps.
struct liveness_loop
{
    plexus::freertos::freertos_timer &timer;
    std::uint32_t ticks{0};

    void arm()
    {
        timer.expires_after(std::chrono::seconds{1});
        timer.async_wait(
            [this](std::error_code)
            {
                ESP_LOGI(k_log_tag, "tick %" PRIu32, ++ticks);
                arm();
            });
    }
};

plexus::node_options telemetry_node_options()
{
    using namespace std::chrono_literals;

    plexus::node_options opts;
    opts.name              = "esp32-lwip-telemetry";
    opts.max_message_bytes = k_max_payload_bytes;
    opts.reconnect         = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x1F1C0DE;
    return opts;
}

void publish_until_stopped(plexus::freertos::freertos_executor &ex, lwip_transport &transport, plexus::node<lwip_policy, lwip_transport> &node)
{
    plexus::publisher<void> telemetry{node, "telemetry"};

    telemetry_source source;
    plexus::freertos::freertos_timer timer(ex);
    publish_loop loop{timer, telemetry, source};
    loop.arm();

    plexus::freertos::freertos_timer tick_timer(ex);
    liveness_loop liveness{tick_timer};
    liveness.arm();

    plexus::freertos::run(ex, transport);
}

void plexus_task(void *)
{
    if(!example::wifi_connect_sta())
        return;

    plexus::freertos::freertos_executor ex;
    lwip_transport transport{ex};
    plexus::discovery::static_discovery disc{{}};
    const plexus::node_options opts = telemetry_node_options();

    transport.use_rx_task(plexus::freertos::task_options{k_rx_task_stack});

    auto node = std::make_unique<plexus::node<lwip_policy, lwip_transport>>(ex, disc, "esp32-lwip-telemetry", transport, opts);
    node->dial({"tcp", k_host_endpoint});
#ifdef PLEXUS_DEAD_ENDPOINT
    node->dial({"tcp", PLEXUS_DEAD_ENDPOINT});
#endif

    publish_until_stopped(ex, transport, *node);
}

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}

#include "wifi_netif.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/multicast_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/lwip_multicast_socket.h"
#include "plexus/freertos/detail/lwip_socket_io.h"
#include "plexus/freertos/detail/lwip_acceptor_io.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_system.h"

#include <span>
#include <array>
#include <string>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <utility>
#include <functional>
#include <system_error>

namespace {

using lwip_socket    = plexus::freertos::detail::lwip_socket;
using lwip_acceptor  = plexus::freertos::detail::lwip_acceptor;
using lwip_policy    = plexus::freertos::lwip_policy<lwip_socket>;
using lwip_transport = plexus::freertos::lwip_transport<lwip_socket, lwip_acceptor>;
using lwip_mcast     = plexus::freertos::lwip_multicast_socket;
using mcast_discovery = plexus::discovery::multicast_discovery<lwip_mcast, lwip_policy>;

constexpr const char *k_group    = "239.255.0.7";
constexpr std::uint16_t k_port    = 7447;
constexpr std::uint8_t  k_ttl     = 4;
constexpr std::uint32_t k_plexus_task_stack = 12288; // bytes
constexpr UBaseType_t   k_plexus_task_prio  = 5;

// A discovery decorator the node consumes: it forwards the abstract surface to the borrowed native
// discovery and tees browse so the node's own awareness wiring AND the gate marker both fire on a
// resolved peer. The decorator owns no mechanism — the false-green guard is the underlying
// multicast_discovery<lwip_multicast_socket, lwip_policy>, constructed below over the lwIP leaf.
class gate_discovery final : public plexus::discovery::discovery
{
public:
    explicit gate_discovery(plexus::discovery::discovery &inner)
            : m_inner(inner)
    {
    }

    void advertise(const plexus::discovery::service_info &service) override
    {
        m_inner.advertise(service);
    }

    void browse(const resolved_callback &on_resolved) override
    {
        m_inner.browse(
                [on_resolved](const plexus::discovery::service_info &peer)
                {
                    std::printf("GATE_PASS_HOST_TO_ESP host=%s name=%s\n", peer.endpoint.address.c_str(), peer.name.c_str());
                    if(on_resolved)
                        on_resolved(peer);
                });
    }

    void on_withdrawn(const withdrawn_callback &cb) override
    {
        m_inner.on_withdrawn(cb);
    }

    void stop() override
    {
        m_inner.stop();
    }

private:
    plexus::discovery::discovery &m_inner;
};

// A 1-byte counter published on the served topic so a dialing-and-subscribing host observes the
// established session. At ~10ms/park, every 100 polls is ~1s — slow enough not to flood the link,
// fast enough that the host gate's accept deadline always covers at least one publish.
struct heartbeat
{
    plexus::publisher<void> &topic;
    std::uint8_t counter{0};
    std::uint32_t ticks{0};

    void poll()
    {
        if(++ticks % 100 != 0)
            return;
        const std::array<std::byte, 1> reading{static_cast<std::byte>(counter)};
        topic.publish(std::span<const std::byte>{reading});
        std::printf("GATE_HEARTBEAT counter=%u\n", static_cast<unsigned>(counter++));
    }
};

// Steady-state heap watch as a pollable: at ~10ms/park, every 500 polls is ~5s. The
// settled free-heap reading must not drift over a sustained RX + discovery window (a zero
// delta proves the lwIP RX + announce/expiry path holds no per-message allocation).
struct heap_watch
{
    std::uint32_t ticks{0};

    void poll()
    {
        if(++ticks % 500 == 0)
            std::printf("HEAP tick=%lu free=%lu\n", static_cast<unsigned long>(ticks),
                        static_cast<unsigned long>(esp_get_free_heap_size()));
    }
};

void plexus_task(void *)
{
    using namespace std::chrono_literals;

    if(!example::wifi_connect_sta())
        return;

    plexus::freertos::freertos_executor ex;

    lwip_mcast disc_sock{ex, k_group, k_port, k_ttl, example::sta_ipv4()};
    mcast_discovery native{ex, disc_sock};
    gate_discovery disc{native};

    lwip_transport transport{ex};

    plexus::node_options opts;
    opts.name      = "esp32-lwip-discovery";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<lwip_policy, lwip_transport> node{ex, disc, "esp32-lwip-discovery", transport, opts};
    node.listen({"tcp", "0.0.0.0:7447"});

    plexus::publisher<void> telemetry{node, "telemetry"};
    heartbeat beat{telemetry};

    heap_watch watch;
    plexus::freertos::run(ex, disc_sock, transport, beat, watch);
}

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}

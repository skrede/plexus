#include "wifi_netif.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/multicast_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/lwip_policy.h"
#include "plexus/freertos/lwip_transport.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/lwip_multicast_socket.h"
#include "plexus/freertos/detail/lwip_socket_io.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <string>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <utility>
#include <functional>
#include <system_error>

namespace {

using lwip_socket    = plexus::freertos::detail::lwip_socket;
using lwip_policy    = plexus::freertos::lwip_policy<lwip_socket>;
using lwip_transport = plexus::freertos::lwip_transport<lwip_socket>;
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

    for(;;)
    {
        disc_sock.poll();
        transport.poll();
        ex.drain();
        ex.park(10ms);
    }
}

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}

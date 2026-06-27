// The host half of the bidirectional on-hardware IGMP gate, driven through the public node facade
// with native multicast discovery. A host node over the asio TCP transport LISTENS (so it announces
// its own card on the multicast group) AND browses; the ESP32 announces over the same group and is
// resolved here, printing GATE_PASS_ESP_TO_HOST with the device's kernel source IP. The host keeps
// running for a window so the device can resolve the host in the reverse direction (the runner reads
// GATE_PASS_HOST_TO_ESP off the device serial). It proves the SAME hoisted multicast_discovery
// template runs on both ends — only the socket leaf differs (asio here, the lwIP leaf on the board).
//
// The discovery LOGIC is identical to the device's: multicast_discovery over a datagram_socket,
// advertise + browse, the endpoint taken from the datagram's unspoofable kernel source. A
// tee-discovery decorator composes the node's awareness wiring with the gate marker so both fire.

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <asio/ip/address_v4.hpp>

#include <string>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <utility>
#include <functional>
#include <string_view>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

constexpr const char *k_group       = "239.255.0.7";
constexpr std::uint16_t k_port       = 7447;
constexpr unsigned      k_ttl        = 4;
constexpr const char *k_default_port = "7447";

// Generous: the device must boot, associate, take a DHCP lease, join the group, then both ends must
// exchange announcements on the 1s cadence — seconds, not the serial gate's sub-second boot.
constexpr std::chrono::seconds k_window{60};

// A discovery decorator that tees browse so the node's awareness wiring AND the gate marker both
// fire on a resolved peer. The underlying mechanism is the same multicast_discovery the device runs.
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
                    std::cout << "GATE_PASS_ESP_TO_HOST device=" << peer.endpoint.address << " name=" << peer.name << '\n'
                              << std::flush;
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

std::string listen_address(int argc, char **argv)
{
    std::string_view port = k_default_port;
    if(argc > 1)
        port = argv[1];
    else if(const char *env = std::getenv("PLEXUS_HOST_PORT"))
        port = env;
    return "0.0.0.0:" + std::string{port};
}

}

int main(int argc, char **argv)
{
    ::asio::io_context io;

    pasio::udp_multicast_socket disc_sock{io, ::asio::ip::make_address_v4(k_group), k_port, k_ttl};
    plexus::discovery::multicast_discovery<pasio::udp_multicast_socket, pasio::asio_policy> native{io, disc_sock};
    gate_discovery disc{native};

    pasio::asio_transport transport{io};

    plexus::node_options opts;
    opts.name      = "multicast-gate-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::asio_policy, pasio::asio_transport> node{io, disc, "multicast-gate-host", transport, opts};
    node.listen({"tcp", listen_address(argc, argv)});

    ::asio::steady_timer window{io, k_window};
    window.async_wait([&](std::error_code) { io.stop(); });

    io.run();
    return 0;
}

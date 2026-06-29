// The host half of the on-device honest-listen gate, driven through the public node facade. The
// device is the SERVER here: it advertises tcp:7447 on the multicast group and binds the real lwIP
// acceptor. This host browses the group, learns the device's source IP, then DIALS the advertised
// tcp:7447, subscribes to the telemetry topic, and runs until one real message arrives over the
// dialed session — proving the advertised card is genuinely served, not a no-op listen.
//
// This is the INVERSE of lwip_gate_host (which listens and lets the device dial): the roles flip, so
// this side calls node.dial, not node.listen. The dial endpoint is the resolved source IP (the
// announcement's unspoofable kernel source) plus the well-known advertised port; the multicast card
// carries the IP, the port is the fixed gate port both ends agree on.

#include "plexus/node.h"
#include "plexus/subscriber.h"
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

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <iostream>
#include <optional>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

constexpr const char *k_group   = "239.255.0.7";
constexpr std::uint16_t k_port    = 7447;
constexpr unsigned      k_ttl     = 4;
constexpr const char *k_topic    = "telemetry";

// Generous: the device must boot, associate, take a DHCP lease, announce on the group, then accept
// the dial and publish — seconds, not the serial gate's sub-second boot.
constexpr std::chrono::seconds k_timeout{60};

// A discovery decorator that tees browse so the node's own awareness wiring AND an injected
// on-peer hook both fire on a resolved peer. The hook is set after the node is constructed (the node
// borrows the discovery at construction), so the gate dials the resolved device from the tee.
class dial_tee final : public plexus::discovery::discovery
{
public:
    explicit dial_tee(plexus::discovery::discovery &inner)
            : m_inner(inner)
    {
    }

    void set_on_peer(resolved_callback cb)
    {
        m_on_peer_cb = std::move(cb);
    }

    void advertise(const plexus::discovery::service_info &service) override
    {
        m_inner.advertise(service);
    }

    void browse(const resolved_callback &on_resolved) override
    {
        m_inner.browse(
                [this, on_resolved](const plexus::discovery::service_info &peer)
                {
                    if(m_on_peer_cb)
                        m_on_peer_cb(peer);
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
    resolved_callback m_on_peer_cb;
};

}

int main()
{
    ::asio::io_context io;

    pasio::udp_multicast_socket disc_sock{io, ::asio::ip::make_address_v4(k_group), k_port, k_ttl};
    plexus::discovery::multicast_discovery<pasio::udp_multicast_socket, pasio::asio_policy> native{io, disc_sock};
    dial_tee disc{native};

    pasio::asio_transport transport{io};

    plexus::node_options opts;
    opts.name      = "lwip-discovery-dial-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::asio_policy, pasio::asio_transport> node{io, disc, "lwip-discovery-dial-host", transport, opts};

    std::optional<std::uint8_t> received;
    plexus::subscriber<> telemetry{node, k_topic,
                                   [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   {
                                       if(!bytes.empty())
                                           received = static_cast<std::uint8_t>(bytes[0]);
                                       io.stop();
                                   }};

    bool dialed = false;
    disc.set_on_peer(
            [&](const plexus::discovery::service_info &peer)
            {
                if(dialed)
                    return;
                dialed = true;
                const std::string endpoint = peer.endpoint.address + ":" + std::to_string(k_port);
                std::cout << "GATE_DIAL device=" << peer.endpoint.address << '\n' << std::flush;
                node.dial({"tcp", endpoint});
            });

    ::asio::steady_timer deadline{io, k_timeout};
    deadline.async_wait([&](std::error_code) { io.stop(); });

    io.run();

    if(received.has_value())
    {
        std::cout << "GATE_PASS value=" << static_cast<unsigned>(*received) << '\n';
        return 0;
    }
    std::cout << "GATE_FAIL no message within " << k_timeout.count() << "s\n";
    return 1;
}

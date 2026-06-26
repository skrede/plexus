// The host half of the on-device lwIP example, driven entirely through the public node facade: a
// node over the host asio TCP transport LISTENS, the ESP32 dials in over Wi-Fi, the host subscribes
// to the telemetry topic, and the executor runs until one real message arrives. It proves the SAME
// node surface the device loop drives also drives the host: a public listen verb, a subscriber, and
// the executor run loop, with no below-facade session construction.
//
// The device is the dialer here (it joins the AP, gets a DHCP lease, then dials the fixed host
// endpoint), so this side binds and accepts: node.listen, not node.dial. The board auto-reset pulse
// is NOT here — the host server holds no serial fd to the board; the runner drives that on the flash
// port. The accept deadline is generous because the device must associate + lease before it dials.

#include "plexus/node.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

constexpr const char *k_topic = "telemetry";
constexpr const char *k_default_port = "7447";

// The device must join the AP, take a DHCP lease, then dial — seconds, not the serial gate's
// sub-second boot. The deadline is generous so a slow association does not false-fail the gate.
constexpr std::chrono::seconds k_timeout{30};

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
    pasio::asio_transport transport{io};

    // The device dials a fixed host endpoint, so the host needs no IP discovery: the table is empty.
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name      = "lwip-gate-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::asio_policy, pasio::asio_transport> node{io, disc, "lwip-gate-host", transport, opts};

    std::optional<std::uint8_t> received;
    plexus::subscriber<> telemetry{node, k_topic,
                                   [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   {
                                       if(!bytes.empty())
                                           received = static_cast<std::uint8_t>(bytes[0]);
                                       io.stop();
                                   }};

    node.listen({"tcp", listen_address(argc, argv)});

    ::asio::steady_timer deadline{io, k_timeout};
    deadline.async_wait([&](std::error_code)
                        { io.stop(); });

    io.run();

    if(received.has_value())
    {
        std::cout << "GATE_PASS value=" << static_cast<unsigned>(*received) << '\n';
        return 0;
    }
    std::cout << "GATE_FAIL no message within " << k_timeout.count() << "s\n";
    return 1;
}

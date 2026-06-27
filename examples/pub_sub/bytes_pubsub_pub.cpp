// Bytes pub/sub — the publisher half. Advertises over native multicast discovery, and
// once a peer is discovered publishes a line of opaque bytes on "demo" every second. Run
// alongside bytes_pubsub_sub in a second shell.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/node_options.h"

#include "plexus/discovery/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include <asio/steady_timer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <functional>

int main()
{
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::asio::udp_multicast_socket mc_socket{io, asio::ip::make_address_v4("239.255.0.7"), 7447, 4};
    plexus::discovery::multicast_discovery<plexus::asio::udp_multicast_socket, plexus::asio::asio_policy>
        disc{io, mc_socket};

    plexus::node_options opts;
    opts.name      = "demo-publisher";
    opts.reconnect = plexus::io::reconnect_config{
        std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0xB17E5;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "demo-publisher", transport, opts};
    node.listen({"tcp", "127.0.0.1:5570"});

    plexus::publisher<> topic{node, "demo", plexus::topic_qos{}, /*emit_source_identity=*/true};

    int n = 0;
    asio::steady_timer tick{io};
    std::function<void()> publish = [&]
    {
        const std::string line = "tick-" + std::to_string(n++);
        topic.publish(std::span<const std::byte>{reinterpret_cast<const std::byte *>(line.data()),
                                                 line.size()});
        tick.expires_after(std::chrono::seconds(1));
        tick.async_wait([&](auto)
                        { publish(); });
    };
    publish();

    io.run();
}

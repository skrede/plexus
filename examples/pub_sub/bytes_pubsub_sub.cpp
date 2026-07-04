// Bytes pub/sub — the subscriber half. Discovers the publisher over native multicast
// discovery and prints every line of bytes that arrives on "demo", plus the source node
// when the producer emits its identity. Run alongside bytes_pubsub_pub in a second shell.

#include "plexus/node.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/io/node_name.h"
#include "plexus/io/message_info.h"

#include "plexus/discovery/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>

int main()
{
    // Flush each line as it is written so a live viewer sees arrivals immediately.
    std::cout.setf(std::ios::unitbuf);
    asio::io_context io;
    plexus::asio::asio_transport transport{io};
    plexus::asio::udp_multicast_socket mc_socket{io, asio::ip::make_address_v4("239.255.0.7"), 7447, 4};
    plexus::discovery::multicast_discovery<plexus::asio::udp_multicast_socket, plexus::asio::asio_policy>
        disc{io, mc_socket};

    plexus::node_options opts;
    opts.name         = "demo-subscriber";
    opts.dial_eagerly = true;
    opts.reconnect    = plexus::io::reconnect_config{
        std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = 0x5DB5C;

    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{
        io, disc, "demo-subscriber", transport, opts};
    node.listen({"tcp", "0.0.0.0:5571"});

    plexus::subscriber<> topic{
        node, "demo", [](std::span<const std::byte> bytes, const plexus::io::message_info &info)
        {
            std::string line{reinterpret_cast<const char *>(bytes.data()), bytes.size()};
            std::cout << "received: " << line;
            if(info.source_identity)
                std::cout << "  (from "
                          << plexus::io::node_name_of(info.source_identity->node_id()) << ")";
            std::cout << '\n';
        }};

    io.run();
}

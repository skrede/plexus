// Zero-config discovery — the out-of-box composition. plexus::asio::default_discovery stands up
// multicast discovery with no group, port, or interface configuration; two nodes built over it
// become mutually aware on one host (multicast loopback) or across a LAN. Run the binary twice:
// `discovery_hello` in one shell and `discovery_hello second` in another; each prints when it has
// noted its peer.

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/default_discovery.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <cstddef>
#include <iostream>
#include <optional>
#include <string_view>

namespace {

plexus::node_id role_id(bool second)
{
    plexus::node_id id{};
    id[0] = second ? std::byte{0xB2} : std::byte{0xA1};
    return id;
}

plexus::node_options role_options(bool second)
{
    plexus::node_options opts;
    opts.name        = second ? "hello-second" : "hello-first";
    opts.reconnect   = plexus::io::reconnect_config{std::chrono::milliseconds(200), std::chrono::seconds(5), std::nullopt, std::nullopt};
    opts.redial_seed = second ? 0xB2F00Du : 0xA1F00Du;
    return opts;
}

}

int main(int argc, char **argv)
{
    const bool second = argc > 1 && std::string_view{argv[1]} == "second";

    asio::io_context io;
    plexus::asio::default_discovery discovery{io};
    plexus::asio::asio_transport transport{io};

    const plexus::node_options opts = role_options(second);
    plexus::node<plexus::asio::asio_policy, plexus::asio::asio_transport> node{io, discovery.discovery(), role_id(second), transport, opts};
    node.listen({"tcp", second ? "0.0.0.0:5581" : "0.0.0.0:5580"});

    const plexus::node_id peer = role_id(!second);
    while(!node.router().known().contains(peer))
        io.run_for(std::chrono::milliseconds(100));
    std::cout << opts.name << " noted its peer with zero discovery configuration\n";

    // A short grace run keeps this node announcing so a peer that started later still notes it.
    io.run_for(std::chrono::seconds(2));
}

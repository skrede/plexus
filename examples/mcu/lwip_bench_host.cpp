// The host half of the on-device bench: an immediate echo peer driven through the public node
// facade. A node over the host asio TCP transport LISTENS, the device dials in over Wi-Fi, and the
// host echoes every request back on the reply topic with no artificial delay — so the device's timed
// round trip is a clean device->host->device measurement. The host-side processing time is part of
// the round trip and is identical across the lwIP-P1 and lwIP-P2 cells, so the P1-vs-P2 delta the
// bench substantiates is unperturbed by it.
//
// This side binds and accepts (the device is the dialer): node.listen, not node.dial. The board
// auto-reset pulse is NOT here — the host holds no serial fd to the board; the runner drives that on
// the flash port. The run loop never returns; the runner kills it once the per-run capture closes.

#include "stream_meter.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
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
#include <cstdlib>
#include <cstddef>
#include <iostream>
#include <string_view>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

constexpr const char *k_request_topic = "request";
constexpr const char *k_reply_topic   = "reply";
constexpr const char *k_stream_topic  = "stream";
constexpr const char *k_default_port  = "7447";

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

    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name      = "lwip-bench-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::asio_policy, pasio::asio_transport> node{io, disc, "lwip-bench-host", transport, opts};

    plexus::publisher<void> reply{node, k_reply_topic};
    plexus::subscriber<void> request{node, k_request_topic,
                                     [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                     { reply.publish(bytes); }};

    example::stream_meter meter;
    plexus::subscriber<void> stream{node, k_stream_topic,
                                    [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                    { ++meter.msgs; meter.bytes += bytes.size(); }};

    ::asio::steady_timer report{io};
    example::schedule_report(report, meter);

    node.listen({"tcp", listen_address(argc, argv)});

    std::cout << "bench host echoing " << k_request_topic << " -> " << k_reply_topic << " on " << listen_address(argc, argv) << std::endl;
    io.run();
    return 0;
}

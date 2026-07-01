// The host half of the bench's serial cell: an immediate echo peer over the host serial transport,
// the serial sibling of lwip_bench_host. A node over the asio serial_transport LISTENS on the second
// USB-serial adapter (the FTDI on UART1, /dev/ttyUSB1 by default); the device is the handshake
// initiator (its bench drive() dials "serial:uart1"), so this side responds via listen, then echoes
// every request back on the reply topic with no artificial delay — the device's timed round trip is a
// clean device->host->device measurement, identical in shape to the lwIP cells.
//
// No board auto-reset here: the FTDI's RTS/DTR are not wired to EN/IO0 (only data + GND), and the
// runner already resets the board on the CP2102 flash port. The run loop never returns; the runner
// kills it once the per-run capture window closes.

#include "stream_meter.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_transport.h"

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
constexpr const char *k_default_link  = "/dev/ttyUSB1";
constexpr const char *k_baud          = "115200";

// The serial endpoint the host listens on: the second adapter's device path with the @baud suffix the
// serial_transport parser expects. Device is argv[1] or LINK_PORT (FTDI-vs-CP2102 enumeration order can
// swap across reboots); baud is argv[2] (default 115200) and MUST match the device's compiled BENCH_BAUD.
std::string link_endpoint(int argc, char **argv)
{
    std::string_view device = k_default_link;
    if(argc > 1)
        device = argv[1];
    else if(const char *env = std::getenv("LINK_PORT"))
        device = env;
    const std::string_view baud = (argc > 2) ? argv[2] : k_baud;
    return std::string{device} + "@" + std::string{baud};
}

}

int main(int argc, char **argv)
{
    ::asio::io_context io;
    pasio::serial_transport transport{io};

    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name      = "serial-bench-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::serial_policy, pasio::serial_transport> node{io, disc, "serial-bench-host", transport, opts};

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

    const std::string endpoint = link_endpoint(argc, argv);
    node.listen({"serial", endpoint});

    std::cout << "serial bench host echoing " << k_request_topic << " -> " << k_reply_topic << " on " << endpoint << '\n';
    io.run();
    return 0;
}

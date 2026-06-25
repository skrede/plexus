// The host half of the on-device serial example, driven entirely through the public node facade: a
// node over the host serial_transport dials the device endpoint, subscribes to the telemetry topic,
// and runs the executor until one real byte arrives. It proves the SAME node surface the device loop
// drives also drives the host: a public dial verb, a subscriber, and the executor run loop, with no
// below-facade session construction and no hand-driven polling.
//
// A serial endpoint (/dev/ttyXXX@baud) is not an IP host:port, so it is not discovery-dialable; the
// node's general dial verb opens the port and drives the handshake the device (the listening
// responder) answers. The lost-boot-handshake race is absorbed by the bounded handshake_retry on the
// node options (Task of the dial milestone) — a re-send, not a busy re-knock loop. The board
// auto-reset RTS/DTR pulse is a host-link concern and stays here (the comms library never touches a
// HAL): it is driven on a raw fd of the same device before the dial, pulsing the board into RUN.

#include "plexus/node.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <span>
#include <string>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

// The CP2102 on UART0 at the board's 8N1 @115200 line discipline. The "serial" scheme + the @baud
// suffix are parsed by the serial_transport endpoint parser.
constexpr const char *k_device_endpoint = "/dev/ttyUSB0@115200";

constexpr const char *k_topic = "telemetry";
constexpr std::chrono::seconds k_timeout{8};

// The bounded handshake re-send budget: each attempt is spaced one handshake_timeout window, so the
// product covers the worst board boot the same way the old knock budget did — without a busy loop.
constexpr std::chrono::seconds k_handshake_window{2};
constexpr std::uint32_t k_handshake_retry{8};

// The ESP32 dev-board auto-reset circuit ties EN/IO0 to the adapter's RTS/DTR; leaving those lines
// asserted holds the board reset-looping. Drive the classic auto-reset-into-RUN sequence on a raw fd
// of the device: IO0 high (run, not the download ROM), then pulse EN low->high. A host-link concern
// only — the device firmware and the wire protocol are untouched.
void reset_board_into_run(std::string_view device)
{
    const int fd = ::open(std::string{device}.c_str(), O_RDWR | O_NOCTTY | O_NONBLOCK);
    if(fd < 0)
        return;
    int dtr = TIOCM_DTR;
    int rts = TIOCM_RTS;
    ::ioctl(fd, TIOCMBIC, &dtr); // IO0 high -> boot the application, not the download ROM
    ::ioctl(fd, TIOCMBIS, &rts); // EN low  -> assert reset
    ::usleep(100 * 1000);
    ::ioctl(fd, TIOCMBIC, &rts); // EN high -> release -> the board boots and runs the app
    ::close(fd);
}

std::string_view device_of(std::string_view endpoint_address)
{
    const auto at = endpoint_address.find('@');
    return at == std::string_view::npos ? endpoint_address : endpoint_address.substr(0, at);
}

}

int main()
{
    ::asio::io_context io;
    pasio::serial_transport transport{io};

    // Point-at-port discovery: the serial link is the only peer, so the table is empty (no IP
    // discovery). The board boots fresh on the reset pulse; the bounded handshake retry then absorbs
    // the boot race, so the reset rides just ahead of the dial.
    plexus::discovery::static_discovery disc{{}};
    reset_board_into_run(device_of(k_device_endpoint));

    plexus::node_options opts;
    opts.name             = "serial-gate-host";
    opts.handshake_timeout = k_handshake_window;
    opts.handshake_retry  = k_handshake_retry;
    opts.reconnect        = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed      = 0x6A7E5;

    plexus::node<pasio::serial_policy, pasio::serial_transport> node{io, disc, "serial-gate-host", transport, opts};

    std::optional<std::uint8_t> received;
    plexus::subscriber<> telemetry{node, k_topic,
                                   [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   {
                                       if(!bytes.empty())
                                           received = static_cast<std::uint8_t>(bytes[0]);
                                       io.stop();
                                   }};

    node.dial({"serial", k_device_endpoint});

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

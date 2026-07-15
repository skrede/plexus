// The host half of the topic-propagation example, driven entirely through the public node facade:
// a node over the host serial_transport dials the device, produces its own typed topic, and asks
// its OWN enumeration surface what the device declared — while the device does the mirror-image ask
// and publishes its answer back. Both directions must land: the host enumerates the device's
// topic-with-type from the propagated declaration, and the device's report says it enumerated the
// host's. Neither end learns a topic from discovery — the broadcast carries identity only.
//
// The board auto-reset RTS/DTR pulse is a host-link concern and stays here (the comms library never
// touches a HAL): it is driven on a raw fd of the same device before the dial, pulsing the board
// into RUN. The lost-boot-handshake race is absorbed by the bounded handshake_retry.

#include "topic_graph_types.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/graph/topic_record.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/reconnect_config.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <span>
#include <array>
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

constexpr std::chrono::seconds k_timeout{12};
constexpr std::chrono::seconds k_handshake_window{2};
constexpr std::uint32_t k_handshake_retry{8};
constexpr std::size_t k_edge_scratch = 8;

// The ESP32 dev-board auto-reset circuit ties EN/IO0 to the adapter's RTS/DTR; leaving those lines
// asserted holds the board reset-looping. Drive the classic auto-reset-into-RUN sequence on a raw fd
// of the device: IO0 high (run, not the download ROM), then pulse EN low->high.
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

// What the two ends each enumerated about the other, as one verdict.
struct verdict
{
    std::string host_saw_device_type;
    std::string device_saw_host_type;
    bool reported{false};
};

bool both_directions_propagated(const verdict &v)
{
    return v.reported && v.host_saw_device_type == example::reading_type::name
           && v.device_saw_host_type == example::command_type::name;
}

void report(const verdict &v)
{
    std::cout << "GATE host_saw=" << (v.host_saw_device_type.empty() ? "<none>" : v.host_saw_device_type)
              << " device_saw=" << (!v.reported ? "<no report>" : v.device_saw_host_type.empty() ? "<none>" : v.device_saw_host_type)
              << '\n';
    std::cout << (both_directions_propagated(v) ? "GATE_PASS both ends enumerate the other's topic-with-type\n"
                                                : "GATE_FAIL topic-with-type did not propagate both ways\n");
}

}

int main()
{
    ::asio::io_context io;
    pasio::serial_transport transport{io};

    // Point-at-port discovery: the serial link is the only peer, so the table is empty. The board
    // boots fresh on the reset pulse; the bounded handshake retry absorbs the boot race.
    plexus::discovery::static_discovery disc{{}};
    reset_board_into_run(device_of(k_device_endpoint));

    plexus::node_options opts;
    opts.name              = "topic-gate-host";
    opts.handshake_timeout = k_handshake_window;
    opts.handshake_retry   = k_handshake_retry;
    opts.reconnect         = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x6A7E5;

    plexus::node<pasio::serial_policy, pasio::serial_transport> node{io, disc, "topic-gate-host", transport, opts};

    verdict seen;
    plexus::publisher<example::command_codec> commands{node, example::k_host_topic};

    // The device's report arrives on the executor, which is where the host's own sweep must run
    // too (the query surface is executor-affine) — so both halves of the verdict are read here.
    plexus::subscriber<> device_report{
        node, example::k_report_topic,
        [&](std::span<const std::byte> bytes, const plexus::io::message_info &)
        {
            std::array<plexus::graph::topic_record, k_edge_scratch> edges{};
            const auto declared = example::declared_type_of(node, example::k_device_topic, plexus::graph::topic_role::publisher,
                                                            std::span<plexus::graph::topic_record>{edges});
            seen.host_saw_device_type.assign(declared);
            seen.device_saw_host_type.assign(reinterpret_cast<const char *>(bytes.data()), bytes.size());
            seen.reported = true;
            io.stop();
        }};

    node.dial({"serial", k_device_endpoint});

    ::asio::steady_timer deadline{io, k_timeout};
    deadline.async_wait([&](std::error_code) { io.stop(); });

    io.run();

    report(seen);
    return both_directions_propagated(seen) ? 0 : 1;
}

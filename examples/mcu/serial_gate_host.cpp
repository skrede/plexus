// The host half of the on-device serial vertical slice: a single dial-and-assert run. It
// reuses the host serial_transport VERBATIM (no new host transport code) on the real CP2102
// /dev/ttyUSB0 link and proves one real message arrives from the attached device through the
// actual handshake + framing + CRC + pub/sub engine.
//
// A serial endpoint (/dev/ttyXXX@baud) is not an IP host:port, so it is not discovery-dialable
// — the node's discovery dial resolves a numeric port per transport, which a serial device has
// no analog of. So the gate drives the engine ONE layer below the node facade, exactly as the
// host serial integration oracle does: the serial_transport opens the port and delivers ONE
// live channel, a peer_session bridges that channel into the REUSED handshake_fsm + pub/sub
// forwarders, and the requester end drives the handshake the device (the listening responder)
// answers. The device publishes a raw single byte (the BOOT-button level, released = 1) on the
// telemetry topic, demand-gated — so the gate subscribes once the session completes and the
// device fans the next sample. on_message captures the byte; a valid one-byte message is the
// pass (it proves the link end to end). On receipt the program prints GATE_PASS value=<v> and
// exits 0; on a few-second timeout it prints GATE_FAIL and exits non-zero. The >=3-run
// reproducibility loop lives in the gate driver, not here.

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_transport.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_info.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include <asio/io_context.hpp>

#include <sys/ioctl.h>
#include <unistd.h>

#include <span>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

using serial_session = pio::peer_session<pasio::serial_policy>;
using serial_msg_fwd = pio::message_forwarder<pasio::serial_policy>;
using serial_rpc_fwd = pio::procedure_forwarder<pasio::serial_policy>;

// The device link: the CP2102 on UART0 at the board's 8N1 @115200 line discipline. The
// "serial" scheme + the @baud suffix are parsed by the serial_transport endpoint parser.
constexpr const char *k_device_endpoint = "/dev/ttyUSB0@115200";

// The topic the device publishes its BOOT-button reading on, and a bound on how long a single
// dial-and-assert run waits for the first message (the device samples at ~1 Hz).
constexpr const char          *k_topic = "telemetry";
constexpr std::chrono::seconds k_timeout{5};

// After releasing the auto-reset lines the board boots fresh; wait for the app to come up (drain
// and resync past the boot-ROM preamble) before driving the handshake request.
constexpr std::chrono::milliseconds k_boot_settle{2500};

// A distinct self-id for the gate's handshake config — any non-zero id satisfies the FSM; the
// link is point-to-point so there is exactly one peer to identify against.
pio::handshake_fsm_config gate_fsm_config()
{
    plexus::node_id id{};
    id[0] = std::byte{0x6E}; // 'n' — a recognizable gate seed, distinct from the device's
    return pio::handshake_fsm_config{.self_id                  = id,
                                     .version_major            = 1,
                                     .version_minor            = 0,
                                     .compatible_version_major = 1,
                                     .compatible_version_minor = 0};
}

// Pump the io_context until pred holds or the deadline passes — the host transport's async read
// loop runs here, draining + resyncing past the un-suppressable boot-ROM preamble.
template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, std::chrono::steady_clock::time_point deadline)
{
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

int main()
{
    using clock = std::chrono::steady_clock;

    ::asio::io_context io;

    // The host serial transport, REUSED unchanged — no new host transport code. It opens the
    // port and hands back ONE live channel through on_dialed (the dialing end drives the
    // handshake, which the device's listening node answers).
    pasio::serial_transport transport{io};

    // The per-peer record OWNS the delivered channel; the session borrows it. node_name is this
    // session's key into the forwarders (the device's display label from the gate's side).
    pio::peer_context<pasio::serial_policy> ctx;
    ctx.node_name = "esp32-telemetry";

    transport.on_dialed(
            [&](std::unique_ptr<pasio::serial_channel> ch, const pio::endpoint &)
            { ctx.channel = std::move(ch); });
    transport.dial({"serial", k_device_endpoint});

    if(!ctx.channel)
    {
        std::cout << "GATE_FAIL could not open " << k_device_endpoint << '\n';
        return 1;
    }

    // The ESP32 dev-board auto-reset circuit ties the chip's EN/IO0 to the adapter's RTS/DTR.
    // Merely opening the port leaves those lines in a state that holds the board reset-looping,
    // so it never runs the app to answer the handshake. Drive the classic auto-reset-into-RUN
    // sequence on the port fd: IO0 high (normal boot, not download), then pulse EN low->high to
    // reset cleanly into the application. A host-link concern only — the device firmware and the
    // wire protocol are untouched.
    {
        const int fd  = ctx.channel->serial_stream().native_handle();
        int       dtr = TIOCM_DTR;
        int       rts = TIOCM_RTS;
        ::ioctl(fd, TIOCMBIC, &dtr); // IO0 high -> boot the application, not the download ROM
        ::ioctl(fd, TIOCMBIS, &rts); // EN low  -> assert reset
        ::usleep(100 * 1000);
        ::ioctl(fd, TIOCMBIC, &rts); // EN high -> release -> the board boots and runs the app
    }
    pump_until(io, [] { return false; }, clock::now() + k_boot_settle);

    // The node-shared pub/sub + rpc forwarders the session bridges this peer into.
    plexus::log::null_logger sink;
    serial_msg_fwd messages{sink};
    serial_rpc_fwd procedures{io, k_timeout};

    // The requester session: is_inbound_bootstrap=false makes this end DRIVE the outbound
    // handshake request the device (the listening responder) answers.
    serial_session session{ctx,      io,       gate_fsm_config(), k_timeout,
                           messages, procedures, /*is_inbound_bootstrap=*/false};

    std::optional<std::uint8_t> received;
    session.on_message(
            [&](std::string_view fqn, std::span<const std::byte> bytes)
            {
                if(fqn == k_topic && !bytes.empty())
                    received = static_cast<std::uint8_t>(bytes[0]);
            });
    session.start();

    // Complete the handshake first, THEN issue the demand-subscribe so the device fans the next
    // telemetry sample to this peer (the publish is demand-gated — no subscriber, no emit).
    const auto deadline = clock::now() + k_timeout;
    pump_until(io, [&] { return session.is_complete(); }, deadline);
    if(!session.is_complete())
    {
        std::cout << "GATE_FAIL handshake did not complete within " << k_timeout.count() << "s\n";
        return 1;
    }

    session.subscribe(k_topic);
    pump_until(io, [&] { return received.has_value(); }, deadline);

    if(received.has_value())
    {
        std::cout << "GATE_PASS value=" << static_cast<unsigned>(*received) << '\n';
        return 0;
    }
    std::cout << "GATE_FAIL no message within " << k_timeout.count() << "s\n";
    return 1;
}

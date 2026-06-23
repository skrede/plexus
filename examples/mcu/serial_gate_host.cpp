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
// and resync past the boot-ROM preamble) before driving the first handshake request.
constexpr std::chrono::milliseconds k_boot_settle{2500};

// The board boots asynchronously after the reset pulse and the engine does NOT retransmit the
// single stream handshake request, so a lone early request is lost. Re-issue it every
// k_attempt_window until the booted board answers, bounded by k_handshake_budget (>> worst boot).
constexpr std::chrono::milliseconds k_attempt_window{1500};
constexpr std::chrono::milliseconds k_handshake_budget{15000};

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

// The ESP32 dev-board auto-reset circuit ties EN/IO0 to the adapter's RTS/DTR; merely opening the
// port leaves those lines holding the board reset-looping. Drive the classic auto-reset-into-RUN
// sequence on the fd: IO0 high (run, not the download ROM), then pulse EN low->high. A host-link
// concern only — the device firmware and the wire protocol are untouched.
void drive_auto_reset_into_run(int fd)
{
    int dtr = TIOCM_DTR;
    int rts = TIOCM_RTS;
    ::ioctl(fd, TIOCMBIC, &dtr); // IO0 high -> boot the application, not the download ROM
    ::ioctl(fd, TIOCMBIS, &rts); // EN low  -> assert reset
    ::usleep(100 * 1000);
    ::ioctl(fd, TIOCMBIC, &rts); // EN high -> release -> the board boots and runs the app
}

// Knock until the booted board answers: each attempt is a fresh requester session on the SAME open
// channel, re-sending the request a predecessor's race lost. The per-attempt window stays under
// k_timeout so the old session is replaced before its abort timer fires (no teardown frame reaches
// the board). Returns true with `session` holding the completed session; a byte lands in `received`.
bool knock_until_handshake(::asio::io_context &io, pio::peer_context<pasio::serial_policy> &ctx,
                           serial_msg_fwd &messages, serial_rpc_fwd &procedures,
                           plexus::log::null_logger &sink, std::optional<serial_session> &session,
                           std::optional<std::uint8_t> &received)
{
    const auto deadline = std::chrono::steady_clock::now() + k_handshake_budget;
    while(std::chrono::steady_clock::now() < deadline)
    {
        session.emplace(ctx, io, gate_fsm_config(), k_timeout, messages, procedures,
                        /*is_inbound_bootstrap=*/false, sink);
        session->on_message(
                [&](std::string_view fqn, std::span<const std::byte> bytes)
                {
                    if(fqn == k_topic && !bytes.empty())
                        received = static_cast<std::uint8_t>(bytes[0]);
                });
        session->start();
        pump_until(io, [&] { return session->is_complete(); },
                   std::min(deadline, std::chrono::steady_clock::now() + k_attempt_window));
        if(session->is_complete())
            return true;
    }
    return false;
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

    // Reset the board into the application, then let it boot before the first handshake request.
    drive_auto_reset_into_run(ctx.channel->serial_stream().native_handle());
    pump_until(io, [] { return false; }, clock::now() + k_boot_settle);

    // The pub/sub + rpc forwarders the session bridges into; they outlive the per-attempt sessions
    // (no forwarder state accrues until a session subscribes, so re-creating one leaves none stale).
    plexus::log::null_logger sink;
    serial_msg_fwd messages{sink};
    serial_rpc_fwd procedures{io, k_timeout, sink};

    std::optional<std::uint8_t>   received;
    std::optional<serial_session> session;
    if(!knock_until_handshake(io, ctx, messages, procedures, sink, session, received))
    {
        std::cout << "GATE_FAIL handshake did not complete within "
                  << k_handshake_budget.count() << "ms\n";
        return 1;
    }

    // Handshake done — demand-subscribe so the device fans its next (demand-gated) telemetry sample.
    session->subscribe(k_topic);
    pump_until(io, [&] { return received.has_value(); }, clock::now() + k_timeout);

    if(received.has_value())
    {
        std::cout << "GATE_PASS value=" << static_cast<unsigned>(*received) << '\n';
        return 0;
    }
    std::cout << "GATE_FAIL no message within " << k_timeout.count() << "s\n";
    return 1;
}

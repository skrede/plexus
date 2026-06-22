#ifndef HPP_GUARD_TESTS_INTEGRATION_SERIAL_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SERIAL_COMMON_H

// The hardware-free host serial loopback oracle. openpty(3) gives a master/slave pty pair the
// kernel wires together exactly like a serial cable — a byte written to one end appears on the
// other — so two ::asio::serial_port objects, one adopting each fd on ONE io_context, are a real
// point-to-point serial link with no device. The pty_link below stands up two serial_channels
// over that pair (the SER-01 framing round-trip) and a peer_session pair over the same channels
// (the SER-04 point-at-port handshake, the requester end dialing the responder bootstrap). Both
// reuse the live channel send/on_data path and the production framing/handshake; nothing is
// hand-stripped. Each behavioral path loops in-body and the ctest invocation is re-run >=3
// process runs — a serial round-trip claim is never made from a single run.

#include "plexus/asio/serial_policy.h"
#include "plexus/asio/serial_channel.h"

#include "plexus/io/frame_router.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/serial_port.hpp>

#include <pty.h>
#include <termios.h>
#include <unistd.h>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using plexus::io::handshake_fsm_config;
using serial_session   = pio::peer_session<pasio::serial_policy>;
using serial_msg_fwd   = pio::message_forwarder<pasio::serial_policy>;
using serial_rpc_fwd   = pio::procedure_forwarder<pasio::serial_policy>;

namespace serial_fixture {

constexpr auto k_long_timeout = std::chrono::hours(1);

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

// Frame a unidirectional "topic" message carrying a chosen session_id through the PRODUCTION
// framing path (a forwarder over an inproc capture link), returning the exact header-on wire
// bytes — handed verbatim to a serial_channel.send() so the round-trip leg exercises real framing,
// not a hand-built frame. Mirrors the peer_session asio oracle's make_data_frame.
inline std::vector<std::byte> make_data_frame(const std::string &payload, std::uint64_t session_id)
{
    using plexus::inproc::inproc_bus;
    using plexus::inproc::inproc_executor;
    using plexus::inproc::inproc_channel;
    using inproc_msg = pio::message_forwarder<plexus::inproc::inproc_policy>;

    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_msg        framer{};
    inproc_channel<>  capture(ex);
    inproc_channel<>  tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    inproc_msg::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

// An openpty master/slave pair. The two fds are the two ends of one virtual serial line; each is
// adopted into an ::asio::serial_port via asio's native-handle ctor. release() hands a fd to the
// serial_port (which then OWNS it and closes it on teardown), so the harness only closes a fd it
// never released — no double close.
struct pty_pair
{
    int master{-1};
    int slave{-1};

    pty_pair()
    {
        REQUIRE(::openpty(&master, &slave, nullptr, nullptr, nullptr) == 0);
        // A default pty is in CANONICAL mode: it line-buffers, echoes, and translates control
        // bytes (NL/CR, IXON flow chars, ISIG). A framed serial stream carries arbitrary binary,
        // so both ends must be RAW byte pipes — exactly what a real serial line is. cfmakeraw
        // clears ICANON/ECHO/ISIG/IXON/OPOST so every byte passes through untransformed.
        make_raw(master);
        make_raw(slave);
    }

    static void make_raw(int fd)
    {
        ::termios tio{};
        REQUIRE(::tcgetattr(fd, &tio) == 0);
        ::cfmakeraw(&tio);
        REQUIRE(::tcsetattr(fd, TCSANOW, &tio) == 0);
    }

    ~pty_pair()
    {
        if(master >= 0)
            ::close(master);
        if(slave >= 0)
            ::close(slave);
    }
    pty_pair(const pty_pair &)            = delete;
    pty_pair &operator=(const pty_pair &) = delete;

    int take_master() { return std::exchange(master, -1); }
    int take_slave() { return std::exchange(slave, -1); }
};

// Adopt a pty fd into a fresh serial_channel: wrap the fd as a serial_port (the native-handle
// ctor — the port now owns the fd) and move it into the adopt-connected channel ctor.
inline std::unique_ptr<pasio::serial_channel>
adopt_channel(::asio::io_context &io, int fd, wire::stream_inbound_config cfg = {})
{
    return std::make_unique<pasio::serial_channel>(io, ::asio::serial_port{io, fd}, cfg);
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred)
{
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

inline void settle(::asio::io_context       &io,
                   std::chrono::milliseconds window = std::chrono::milliseconds(20))
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

#endif

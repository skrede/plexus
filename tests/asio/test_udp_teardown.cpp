// Teardown-discipline regression for the connectionless UDP transport demux lifetime.
//
// The transport shares ONE bound socket and demuxes inbound datagrams in userspace
// (sender endpoint -> udp_channel*). On a successful accept/dial the channel's
// unique_ptr is moved OUT to the engine while the demux keeps a NON-owning raw ref so
// inbound keeps routing. If the engine destroys that completed channel mid-session
// (any normal session teardown) BEFORE transport::close(), the next datagram from that
// source must NOT dereference the freed pointer in on_datagram -> lookup ->
// deliver_inbound. The structural fix erases the demux entry when the channel tears
// down; this case is the proof (a heap-use-after-free under asan on the pre-fix code).
//
// The current udp transport suite never destroys a live completed channel mid-session,
// so it never reaches this path. Looped to shake out any nondeterminism; the asan tree
// (-fsanitize=address,undefined -fno-sanitize-recover=all) is where the UAF is caught.

#include "udp_loopback.h"

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/transport_backend.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using plexus::testing::loopback;

namespace {

static_assert(pio::byte_channel<pasio::udp_channel>);
static_assert(pio::transport_backend<pasio::udp_transport, pasio::udp_policy>);

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

}

TEST_CASE("udp.teardown: an inbound datagram after the engine destroys a completed accepted "
          "channel is safe, looped",
          "[udp][teardown]")
{
    // Complete the accept, then destroy the engine-owned accepted channel mid-session
    // (the natural session-teardown reaction) and deliver a fresh datagram from the
    // SAME source. Pre-fix: the demux still holds the freed accepted-channel pointer,
    // so on_datagram -> lookup -> deliver_inbound is a heap-use-after-free (asan). The
    // fix erases the demux entry on teardown, so the datagram resolves to a clean MISS.
    constexpr int k_iterations = 50;
    int survived               = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        loopback h;
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        // Engine teardown: destroy the completed accepted channel while the dialer (and
        // the demux entry keyed on the dialer's source endpoint) are still live.
        h.accepted.reset();
        h.drain();

        // The dialer sends another frame: it arrives at the server from the SAME source
        // endpoint the demux is keyed on. The torn-down entry must be gone (a clean MISS),
        // never a freed-pointer deref.
        auto frame = bytes_of("after-teardown-" + std::to_string(i));
        h.dialed->send(frame);
        h.drain();

        ++survived; // reaching here without an asan UAF abort is the proof
    }
    REQUIRE(survived == k_iterations);
}

TEST_CASE("udp.teardown: the transport stays healthy for a NEW peer after a completed channel "
          "tears down",
          "[udp][teardown]")
{
    // After an engine teardown erases the torn-down entry, a genuinely new dialer must
    // still accept cleanly — the teardown erase must not corrupt the demux or the
    // transport's accept path.
    ::asio::io_context io;
    pasio::udp_transport server{io};

    std::vector<std::unique_ptr<pasio::udp_channel>> accepted;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted.push_back(std::move(ch)); });
    server.listen({"udp", "127.0.0.1:0"});

    plexus::testing::pump_until(io, [&] { return server.port() != 0; }, std::chrono::seconds(5));

    pasio::udp_transport first{io};
    std::unique_ptr<pasio::udp_channel> first_dialed;
    first.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { first_dialed = std::move(ch); });
    first.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return first_dialed != nullptr && accepted.size() == 1; }, std::chrono::seconds(5));
    REQUIRE(accepted.size() == 1);

    // Tear the first accepted channel down, then bring up a fresh dialer.
    accepted.clear();
    for(int i = 0; i < 256; ++i)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }

    pasio::udp_transport second{io};
    std::unique_ptr<pasio::udp_channel> second_dialed;
    second.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { second_dialed = std::move(ch); });
    second.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return second_dialed != nullptr && accepted.size() == 1; }, std::chrono::seconds(5));

    REQUIRE(second_dialed != nullptr);
    REQUIRE(accepted.size() == 1); // the new peer accepted cleanly post-teardown
}

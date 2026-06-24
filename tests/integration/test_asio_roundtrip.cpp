#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

namespace {

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

// Run one full subscribe-publish-receive round-trip over real TCP loopback.
// Returns the opaque payload the subscriber received (the inner unidirectional
// data the reassembler delivered), or nullopt if nothing arrived. Each call
// stands up a fresh listener + client on its own io_context so the iterations
// are independent.
std::optional<std::vector<std::byte>> one_roundtrip(std::span<const std::byte> payload, std::string_view fqn)
{
    ::asio::io_context io;

    // Server side: a listener accepts a channel the publisher fans out over.
    pasio::asio_listener listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch) { server_channel = std::move(ch); });
    listener.start({"tcp", "127.0.0.1:0"});
    auto port = listener.port();

    // Client side: dial the listener and read the framed message. The asio
    // channel now delivers a COMPLETE header-on frame to on_data (it re-frames
    // each reassembled frame), so the client routes it through a frame_router —
    // the router owns the frame_header strip + type switch and hands the inner
    // unidirectional payload to its consumer. This exercises the unified receive
    // contract honestly rather than hand-stripping the header in the test.
    pasio::asio_channel client(io);
    std::optional<std::vector<std::byte>> received;
    plexus::log::null_logger router_sink;
    pio::frame_router router{router_sink};
    router.on_unidirectional(
            [&](const wire::frame_header &, std::span<const std::byte> inner)
            {
                auto decoded = wire::decode_unidirectional(inner);
                if(decoded)
                    received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
            });
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    // Pump the context until the accept completes so server_channel is live.
    while(!server_channel)
        io.poll_one();

    // The publisher-side forwarder fans toward the accepted server channel.
    // attach emits a frame_header-wrapped subscribe control frame: control frames
    // are now framed identically to data, so the reassembler frames the subscribe
    // cleanly (it no longer rejects-and-clears on it) and it reassembles alongside
    // the data frame. The client's frame_router demuxes by frame_header.type — the
    // subscribe has no registered consumer and is warn-and-dropped, the
    // unidirectional data routes to its consumer — so no flush loop is needed.
    plexus::log::null_logger sink;
    pio::message_forwarder<pasio::asio_policy> fwd{sink};
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "client-node"};
    fwd.attach(sub, fqn);

    fwd.publish(fqn, payload);

    // Drive the context until the payload arrives. A GENEROUS wall-clock backstop, not
    // a tight deadline: the happy path returns the instant the payload lands, so a wide
    // bound only keeps a contended host from slipping the clock before the
    // (completed-anyway) round-trip is observed; a real regression still fails on the
    // predicate never coming true.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while(!received && std::chrono::steady_clock::now() < deadline)
        io.poll();

    return received;
}

}

TEST_CASE("publish round-trips over real TCP loopback through plexus-asio", "[integration][asio]")
{
    const auto payload    = bytes_of("plexus-live-tcp-payload");
    const std::string fqn = "demo._plexus._tcp.local.";

    // Run the FULL round-trip >=100 times in-body: a live networking claim must
    // be proven reproducible, never asserted from a single run. Each iteration
    // is an independent listener+client+io_context, so a flaky frame would
    // surface as a mismatch on some iteration, not a one-off pass.
    constexpr int k_iterations = 100;
    int delivered              = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto got = one_roundtrip(payload, fqn);
        REQUIRE(got.has_value());
        REQUIRE(*got == payload); // the exact opaque bytes, every iteration
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

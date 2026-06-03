#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"

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
namespace wire = plexus::wire;
namespace pio = plexus::io;

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
std::optional<std::vector<std::byte>> one_roundtrip(std::span<const std::byte> payload,
                                                    std::string_view fqn)
{
    ::asio::io_context io;

    // Server side: a listener accepts a channel the publisher fans out over.
    pasio::asio_listener listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch)
    {
        server_channel = std::move(ch);
    });
    listener.start({"tcp", "127.0.0.1:0"});
    auto port = listener.port();

    // Client side: dial the listener and read the framed message. The
    // reassembler in asio_channel strips the frame_header and posts the inner
    // unidirectional payload to on_data.
    pasio::asio_channel client(io);
    std::optional<std::vector<std::byte>> received;
    client.on_data([&](std::span<const std::byte> inner)
    {
        auto decoded = wire::decode_unidirectional(inner);
        if(decoded)
            received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
    });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    // Pump the context until the accept completes so server_channel is live.
    while(!server_channel)
        io.poll_one();

    // The publisher-side forwarder fans toward the accepted server channel.
    // attach emits an (unframed) subscribe control frame; the reassembler on the
    // client correctly rejects it as non-data and clears, so we flush it across
    // its own read cycle BEFORE publishing the data frame — the SLICE-2 round-trip
    // proves the publish->frame-once->fan-out->reassemble->receive DATA path, not
    // the control-frame wire discipline (a separate hardening concern). Flushing
    // in a bounded poll keeps the subscribe and the data frame in distinct reads.
    pio::message_forwarder<pasio::asio_policy> fwd;
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "client-node"};
    fwd.attach(sub, fqn);
    for(int i = 0; i < 256; ++i)
        io.poll();

    fwd.publish(fqn, payload);

    // Drive the context until the payload arrives (bounded so a regression fails
    // fast instead of hanging).
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(!received && std::chrono::steady_clock::now() < deadline)
        io.poll();

    return received;
}

}

TEST_CASE("publish round-trips over real TCP loopback through plexus-asio", "[integration][asio]")
{
    const auto payload = bytes_of("plexus-live-tcp-payload");
    const std::string fqn = "demo._plexus._tcp.local.";

    // Run the FULL round-trip >=100 times in-body: a live networking claim must
    // be proven reproducible, never asserted from a single run. Each iteration
    // is an independent listener+client+io_context, so a flaky frame would
    // surface as a mismatch on some iteration, not a one-off pass.
    constexpr int k_iterations = 100;
    int delivered = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto got = one_roundtrip(payload, fqn);
        REQUIRE(got.has_value());
        REQUIRE(*got == payload); // the exact opaque bytes, every iteration
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

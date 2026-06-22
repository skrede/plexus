#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

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

// Run one frame_header-wrapped CONTROL-frame round-trip over real TCP loopback.
// The publisher-side message_forwarder.attach emits a framed subscribe over the
// REAL asio_channel (hence the REAL reassembler); the receiving channel's on_data
// feeds a frame_router. Returns the inner subscribe payload the router demuxed
// (proving the frame_header.type byte SURVIVED the asio receive path), or nullopt.
std::optional<wire::subscribe_request> one_control_roundtrip(std::string_view fqn)
{
    ::asio::io_context io;

    pasio::asio_listener                 listener(io);
    std::unique_ptr<pasio::asio_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::asio_channel> ch)
                         { server_channel = std::move(ch); });
    listener.start({"tcp", "127.0.0.1:0"});
    auto port = listener.port();

    // The receiving side routes its header-on frames through a frame_router; the
    // subscribe consumer decodes the inner payload (header-off, the router
    // stripped the frame_header).
    pasio::asio_channel                    client(io);
    std::optional<wire::subscribe_request> demuxed;
    pio::frame_router                      router;
    router.on_subscribe(
            [&](std::span<const std::byte> inner)
            {
                if(auto req = wire::decode_subscribe_request(inner))
                    demuxed = req;
            });
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), port);
    client.socket().connect(server_ep);
    client.start_read();

    while(!server_channel)
        io.poll_one();

    // attach emits a frame_header-wrapped subscribe toward the accepted channel.
    plexus::log::null_logger                         sink;
    pio::message_forwarder<pasio::asio_policy>       fwd{sink};
    pio::message_forwarder<pasio::asio_policy>::peer sub{*server_channel, "client-node"};
    fwd.attach(sub, fqn);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while(!demuxed && std::chrono::steady_clock::now() < deadline)
        io.poll();

    return demuxed;
}

}

TEST_CASE(
        "a framed control frame demuxes through the real asio reassembler + frame_router over TCP",
        "[integration][asio][router]")
{
    const std::string fqn           = "demo._plexus._tcp.local.";
    const auto        expected_hash = wire::fqn_topic_hash(fqn);

    // Loop the round-trip in-body: a live-networking claim is never asserted from
    // a single run. Each iteration is an independent listener+client+io_context,
    // so the type byte surviving the asio receive path is proven reproducible.
    // (The deeper N>=100 + multi-run discipline stays with the req/res roundtrip.)
    constexpr int k_iterations = 20;
    int           demuxed      = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto req = one_control_roundtrip(fqn);
        REQUIRE(req.has_value());                  // the subscribe consumer fired
        REQUIRE(req->topic_hash == expected_hash); // with the correct inner payload
        REQUIRE(req->fqn == fqn);
        ++demuxed;
    }
    REQUIRE(demuxed == k_iterations);
}

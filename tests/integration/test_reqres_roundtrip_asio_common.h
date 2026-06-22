#ifndef HPP_GUARD_TESTS_INTEGRATION_REQRES_ROUNDTRIP_ASIO_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_REQRES_ROUNDTRIP_ASIO_COMMON_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using forwarder = pio::procedure_forwarder<pasio::asio_policy>;
using wire::rpc_status;

namespace reqres_asio_fixture {

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A live req/res harness over real TCP loopback: a server-side provider and a
// client-side caller on ONE io_context. The single connection is bidirectional —
// the client sends requests + receives responses, the server receives requests +
// sends responses. Each side's on_data is wired DIRECTLY to a frame_router (the
// settled header-on receive contract: on_data delivers a COMPLETE header-on frame,
// route() strips the frame_header and demuxes by type). There is NO re-wrap, no
// header-strip reconciliation, no flush hack — the unified framing carries the
// control + rpc frames intact through the reassembler.
struct live_rpc
{
    ::asio::io_context                   io;
    pasio::asio_listener                 listener{io};
    std::unique_ptr<pasio::asio_channel> server_channel;
    pasio::asio_channel                  client{io};
    plexus::log::null_logger             sink;

    pio::frame_router server_router; // server: demux inbound rpc_request
    pio::frame_router client_router; // client: demux inbound rpc_response

    std::optional<forwarder> provider; // server side (constructed once accepted)
    forwarder                caller{
            io, std::chrono::seconds(30),
            sink}; // client side; generous so the roundtrip never trips

    std::optional<forwarder::peer> caller_peer;
    std::optional<forwarder::peer> provider_peer;

    live_rpc()
    {
        listener.on_accepted([this](std::unique_ptr<pasio::asio_channel> ch)
                             { server_channel = std::move(ch); });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.socket().connect(server_ep);

        // Client receive: header-on frame -> router -> rpc_response -> caller.
        client_router.on_rpc_response([this](std::span<const std::byte> inner)
                                      { caller.deliver_response(*caller_peer, inner); });
        client.on_data([this](std::span<const std::byte> frame) { client_router.route(frame); });
        client.start_read();

        // Pump until the accept completes so the server channel is live.
        while(!server_channel)
            io.poll_one();

        caller_peer.emplace(forwarder::peer{client, "server-node"});
        provider.emplace(io, std::chrono::seconds(30), sink);
        provider_peer.emplace(forwarder::peer{*server_channel, "client-node"});

        // Server receive: header-on frame -> router -> rpc_request -> provider.
        server_router.on_rpc_request([this](std::span<const std::byte> inner)
                                     { provider->deliver_request(*provider_peer, inner); });
        server_channel->on_data([this](std::span<const std::byte> frame)
                                { server_router.route(frame); });
        server_channel->start_read();
    }

    // Pump the io_context until pred() or a bounded deadline (so a regression fails
    // fast rather than hanging).
    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while(!pred() && std::chrono::steady_clock::now() < deadline)
            io.poll();
    }
};

}

#endif

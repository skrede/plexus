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
namespace wire = plexus::wire;
namespace pio = plexus::io;

using forwarder = pio::procedure_forwarder<pasio::asio_policy>;
using wire::rpc_status;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
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
    ::asio::io_context io;
    pasio::asio_listener listener{io};
    std::unique_ptr<pasio::asio_channel> server_channel;
    pasio::asio_channel client{io};

    pio::frame_router server_router;   // server: demux inbound rpc_request
    pio::frame_router client_router;   // client: demux inbound rpc_response

    std::optional<forwarder> provider;     // server side (constructed once accepted)
    forwarder caller;                      // client side

    std::optional<forwarder::peer> caller_peer;
    std::optional<forwarder::peer> provider_peer;

    live_rpc()
    {
        listener.on_accepted([this](std::unique_ptr<pasio::asio_channel> ch) {
            server_channel = std::move(ch);
        });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.socket().connect(server_ep);

        // Client receive: header-on frame -> router -> rpc_response -> caller.
        client_router.on_rpc_response([this](std::span<const std::byte> inner) {
            caller.deliver_response(*caller_peer, inner);
        });
        client.on_data([this](std::span<const std::byte> frame) { client_router.route(frame); });
        client.start_read();

        // Pump until the accept completes so the server channel is live.
        while(!server_channel)
            io.poll_one();

        caller_peer.emplace(forwarder::peer{client, "server-node"});
        provider.emplace();
        provider_peer.emplace(forwarder::peer{*server_channel, "client-node"});

        // Server receive: header-on frame -> router -> rpc_request -> provider.
        server_router.on_rpc_request([this](std::span<const std::byte> inner) {
            provider->deliver_request(*provider_peer, inner);
        });
        server_channel->on_data([this](std::span<const std::byte> frame) { server_router.route(frame); });
        server_channel->start_read();
    }

    // Pump the io_context until pred() or a bounded deadline (so a regression fails
    // fast rather than hanging).
    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while(!pred() && std::chrono::steady_clock::now() < deadline)
            io.poll();
    }
};

}

TEST_CASE("req/res round-trips over real TCP loopback through plexus-asio, looped",
          "[integration][reqres][asio]")
{
    // Loop the FULL roundtrip >=100 times in-body, each iteration an independent
    // listener+client+io_context — a live-networking claim is never asserted from a
    // single run. A flaky frame surfaces as a mismatch on some iteration, not a
    // one-off pass. The ctest invocation is ALSO repeated >=3 process runs (the
    // CMake verify loop) for cross-process reproducibility.
    constexpr int k_iterations = 100;
    int resolved = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_rpc h;

        std::string seen_param;
        h.provider->serve("svc", [&](std::span<const std::byte> param, forwarder::reply_fn &reply) {
            seen_param = to_string(param);
            const std::string ret = "return-" + seen_param;
            reply(rpc_status::success, as_bytes(ret));
        });

        rpc_status got_status = rpc_status::error;
        std::string got_return;
        const std::string param = "param-" + std::to_string(iter);
        h.caller.call(*h.caller_peer, "svc", as_bytes(param),
            [&](rpc_status s, std::span<const std::byte> ret) {
                got_status = s;
                got_return = to_string(ret);
            });

        h.pump_until([&] { return got_status != rpc_status::error; });

        REQUIRE(seen_param == param);                       // provider saw the exact param
        REQUIRE(got_status == rpc_status::success);
        REQUIRE(got_return == "return-" + param);           // caller matched the exact return
        ++resolved;
    }
    REQUIRE(resolved == k_iterations);
}

TEST_CASE("concurrent outstanding req/res over real TCP each resolve to their own response, looped",
          "[integration][reqres][asio]")
{
    // The key staged-context stress over REAL asio TCP: issue M>=8 overlapping
    // calls (distinct correlation_ids) BEFORE pumping, so multiple requests are
    // in flight before any response arrives. The provider echoes the param, so a
    // clobber (the caller matching the wrong response, or the provider's staged
    // reply context being overwritten across dispatches) would surface as a
    // mismatched echo on some call. Asserting each callback resolved to ITS OWN
    // response proves no cross-talk over the wire.
    constexpr int k_iterations = 100;
    constexpr int m_outstanding = 8;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_rpc h;
        h.provider->serve("echo", [](std::span<const std::byte> param, forwarder::reply_fn &reply) {
            reply(rpc_status::success, param);
        });

        std::array<std::string, m_outstanding> got{};
        std::array<rpc_status, m_outstanding> status{};
        status.fill(rpc_status::error);
        for(int i = 0; i < m_outstanding; ++i)
        {
            const std::string param = "req-" + std::to_string(iter) + "-" + std::to_string(i);
            h.caller.call(*h.caller_peer, "echo", as_bytes(param),
                [&got, &status, i](rpc_status s, std::span<const std::byte> ret) {
                    status[i] = s;
                    got[i] = to_string(ret);
                });
        }

        int done = 0;
        h.pump_until([&] {
            done = 0;
            for(int i = 0; i < m_outstanding; ++i)
                if(status[i] != rpc_status::error)
                    ++done;
            return done == m_outstanding;
        });

        for(int i = 0; i < m_outstanding; ++i)
        {
            REQUIRE(status[i] == rpc_status::success);
            REQUIRE(got[i] == "req-" + std::to_string(iter) + "-" + std::to_string(i));
        }
    }
}

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
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using forwarder = pio::procedure_forwarder<pasio::asio_policy>;
using wire::rpc_status;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A live req/res harness over real TCP loopback whose provider NEVER replies: the
// server-side handler is dispatched over the wire but drops its reply on the floor,
// so the only thing that can resolve the caller's outstanding request is its own
// per-call deadline timer (a real asio steady_timer). The connection is a single
// bidirectional TCP link, exactly as the roundtrip harness, so the timeout is
// proven over the production receive path — not a synthetic stub.
struct silent_rpc
{
    ::asio::io_context                   io;
    pasio::asio_listener                 listener{io};
    std::unique_ptr<pasio::asio_channel> server_channel;
    pasio::asio_channel                  client{io};
    plexus::log::null_logger             sink;

    pio::frame_router server_router;
    pio::frame_router client_router;

    std::optional<forwarder> provider;
    std::optional<forwarder> caller;

    std::optional<forwarder::peer> caller_peer;
    std::optional<forwarder::peer> provider_peer;

    explicit silent_rpc(std::chrono::nanoseconds caller_deadline)
    {
        listener.on_accepted([this](std::unique_ptr<pasio::asio_channel> ch)
                             { server_channel = std::move(ch); });
        listener.start({"tcp", "127.0.0.1:0"});

        ::asio::ip::tcp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), listener.port());
        client.socket().connect(server_ep);

        caller.emplace(io, caller_deadline, sink);
        client_router.on_rpc_response([this](std::span<const std::byte> inner)
                                      { caller->deliver_response(*caller_peer, inner); });
        client.on_data([this](std::span<const std::byte> frame) { client_router.route(frame); });
        client.start_read();

        while(!server_channel)
            io.poll_one();

        caller_peer.emplace(forwarder::peer{client, "server-node"});
        provider.emplace(io, std::chrono::hours(1), sink); // provider arms no timeout of its own
        provider_peer.emplace(forwarder::peer{*server_channel, "client-node"});

        // The provider receives + dispatches the request, but the handler NEVER
        // replies (drops the reply&) — the genuine never-answers state.
        provider->serve("svc", [](std::span<const std::byte>, forwarder::reply_fn &) {});

        server_router.on_rpc_request([this](std::span<const std::byte> inner)
                                     { provider->deliver_request(*provider_peer, inner); });
        server_channel->on_data([this](std::span<const std::byte> frame)
                                { server_router.route(frame); });
        server_channel->start_read();
    }

    // Pump the io_context until pred() or a generous bounded wall-clock deadline, so
    // a regression (the timeout never firing) fails fast rather than hanging.
    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

}

TEST_CASE("a call whose real TCP provider never replies times out over live loopback, looped",
          "[integration][reqres][asio][timeout]")
{
    // The reproducible asio proof: a provider that receives the request over REAL TCP
    // but never replies; a short-but-generous per-call deadline (150 ms — well clear
    // of loopback dispatch jitter, yet small wall-time); the caller's asio
    // steady_timer must fire rpc_status::timeout. Looped >=20 in-body so a flaky
    // timer surfaces as a miss on some iteration rather than a one-off pass; the
    // ctest invocation is ALSO re-run >=3 process runs for cross-process
    // reproducibility (a live-timing claim is never made from a single run).
    constexpr int k_iterations = 20;
    const auto    deadline     = std::chrono::milliseconds(150);
    int           timed_out    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        silent_rpc h(deadline);

        rpc_status got   = rpc_status::error;
        int        fired = 0;
        h.caller->call(*h.caller_peer, "svc", as_bytes(std::string{"param"}),
                       [&](rpc_status s, std::span<const std::byte>)
                       {
                           ++fired;
                           got = s;
                       });

        h.pump_until([&] { return fired > 0; });

        REQUIRE(fired == 1);                 // resolved exactly once
        REQUIRE(got == rpc_status::timeout); // and by the deadline, not a reply
        ++timed_out;
    }
    REQUIRE(timed_out == k_iterations);
}

TEST_CASE("a per-call deadline override outlives the forwarder default over live loopback",
          "[integration][reqres][asio][timeout]")
{
    // Prove the per-call override path on the live backend: a forwarder default of an
    // hour (so the default would NOT fire within the test) with a SHORT per-call
    // override — the override is what arms the timer, so the call still times out
    // promptly. Looped; re-run across process runs by the ctest verify loop.
    constexpr int k_iterations = 20;
    int           timed_out    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        silent_rpc h(std::chrono::hours(1)); // default deadline far in the future

        rpc_status got   = rpc_status::error;
        int        fired = 0;
        h.caller->call(
                *h.caller_peer, "svc", as_bytes(std::string{"param"}),
                [&](rpc_status s, std::span<const std::byte>)
                {
                    ++fired;
                    got = s;
                },
                std::chrono::milliseconds(150)); // per-call override arms the short timer

        h.pump_until([&] { return fired > 0; });

        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::timeout);
        ++timed_out;
    }
    REQUIRE(timed_out == k_iterations);
}

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

using plexus::io::handshake_fsm_config;
using session = pio::peer_session<pasio::asio_policy>;
using msg_forwarder = pio::message_forwarder<pasio::asio_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::asio_policy>;

static_assert(plexus::io::transport_backend<pasio::asio_transport, pasio::asio_policy>);

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A DIALED TCP loopback link turned into a peer_session pair — NO hand-dial. The
// transport's listen(ep) opens the acceptor; dial(ep) async-connects, and the two
// ends arrive via on_accepted (the server end, is_inbound_bootstrap=true) and
// on_dialed (the client end, is_inbound_bootstrap=false). The sessions are
// deferred in std::optional and built only after the channels land. A real
// published message rides the same live TCP channels post-handshake. Each
// instance stands up a fresh transport + io_context so the looped iterations are
// independent.
struct dial_tcp_link
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};

    msg_forwarder req_messages;
    msg_forwarder resp_messages;
    rpc_forwarder req_procedures{io, k_long_timeout};
    rpc_forwarder resp_procedures{io, k_long_timeout};

    plexus::io::peer_context<pasio::asio_policy> req_ctx;   // the dialer slot's per-peer record
    plexus::io::peer_context<pasio::asio_policy> resp_ctx;  // the accepted slot's per-peer record
    std::optional<session> requester;   // the dialer (client) end
    std::optional<session> responder;   // the accepted (server) end

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    explicit dial_tcp_link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted([this, timeout](std::unique_ptr<pasio::asio_channel> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            responder.emplace(resp_ctx, io, make_cfg(0x01), timeout,
                              resp_messages, resp_procedures, true);
            responder->on_message([this](std::string_view, std::span<const std::byte> d) {
                resp_received.emplace_back(to_string(d));
            });
            responder->start();
        });
        transport.on_dialed([this, timeout](std::unique_ptr<pasio::asio_channel> ch) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            requester.emplace(req_ctx, io, make_cfg(0x02), timeout,
                              req_messages, req_procedures, false);
            requester->on_message([this](std::string_view, std::span<const std::byte> d) {
                req_received.emplace_back(to_string(d));
            });
            requester->start();
        });

        transport.listen({"tcp", "127.0.0.1:0"});
        transport.dial({"tcp", "127.0.0.1:" + std::to_string(transport.port())});
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

}

TEST_CASE("asio transport: a DIALED peer_session pair completes the handshake over real TCP and mints epochs, looped",
          "[integration][transport][asio]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_tcp_link l;
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });

        REQUIRE(l.requester.has_value());
        REQUIRE(l.responder.has_value());
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("asio transport: a real published message carrying the minted epoch flows post-dial over TCP, looped",
          "[integration][transport][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload = "dialed-published-bytes-over-tcp";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_tcp_link l;
        l.pump_until([&] { return l.requester && l.responder
                                  && l.requester->is_complete() && l.responder->is_complete(); });
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.settle();   // drain the subscribe handshake
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());

        l.pump_until([&] { return !l.resp_received.empty(); });
        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

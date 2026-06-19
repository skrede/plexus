// Gated real-TCP reconnect oracle over asio loopback. The reconnect driver drives
// the dial-retry cycle on a real steady_timer: an established session whose channel
// drops (the socket is closed on the accepted end, surfacing broken_pipe /
// connection_reset on the dialer) tears down, backs off, re-dials through
// asio_transport::dial, and re-handshakes to a fresh epoch; a dial to a
// CLOSED port re-dials until a listener appears. The deterministic
// surrender / ceiling sweeps live on the inproc virtual clock (the sibling oracle);
// this leg validates the chosen numbers behave on the steady timer. The full
// scenario loops in-body and the ctest invocation is re-run >=3 process runs (a
// live-networking claim is never made from one run). Links plexus::inproc for the
// forwarder framing path.

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/reconnect.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/epoch_source.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using pio::handshake_fsm_config;
using pio::reconnect_config;
using session       = pio::peer_session<pasio::asio_policy>;
using msg_forwarder = pio::message_forwarder<pasio::asio_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::asio_policy>;
using driver_t =
        pio::reconnect<pasio::asio_policy, pasio::asio_transport, std::chrono::steady_clock>;

namespace {

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

// One reconnecting dialer over real TCP loopback. The listener stays up for the
// harness lifetime; the driver re-dials it after a drop. The dialer channel's
// on_error routes the transport drop to the driver (NOT a clean close); on_redial
// tears the dead requester down and carries its epoch forward so the next
// completion mints a strictly later epoch. The accepted (responder) end is rebuilt
// fresh per accept. Members are ordered io/transport BEFORE the channels so
// destruction unwinds channels before the io_context.
struct tcp_reconnect
{
    ::asio::io_context    io;
    pasio::asio_transport transport{io};

    msg_forwarder req_messages{};
    msg_forwarder resp_messages{};
    rpc_forwarder req_procedures{io, k_long_timeout};
    rpc_forwarder resp_procedures{io, k_long_timeout};

    // The per-peer records own the channel + the epoch well and OUTLIVE every
    // incarnation, so each rebuilt session draws a strictly-later epoch with no
    // hand-off of the dead one. The redial driver is a harness-owned SIBLING of the
    // requester's record (NOT a record member); both are declared BEFORE the
    // std::optional sessions so destruction unwinds the session first.
    plexus::io::peer_context<pasio::asio_policy> req_ctx;
    plexus::io::peer_context<pasio::asio_policy> resp_ctx;
    std::optional<driver_t>                      driver;
    std::optional<session>                       requester;
    std::optional<session>                       responder;

    std::uint16_t port{0};
    int           drops_seen{0};

    // listen_first: bind the listener on an ephemeral port BEFORE the driver so the
    // driver dials the real port. When false the caller supplies a (closed) port to
    // exercise the initial-refused path and brings the listener up later.
    tcp_reconnect(const reconnect_config &cfg, bool listen_first, std::uint16_t closed_port = 0)
    {
        transport.on_accepted(
                [this](std::unique_ptr<pasio::asio_channel> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, io, make_cfg(0x01), k_long_timeout, resp_messages,
                                      resp_procedures, true);
                    responder->start();
                });
        transport.on_dialed(
                [this](std::unique_ptr<pasio::asio_channel> ch, const pio::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, io, make_cfg(0x02), k_long_timeout, req_messages,
                                      req_procedures, false);
                    requester->start();
                    // Route a transport DROP (broken_pipe/connection_reset) — not a clean
                    // close — to the driver through the session's production drop seam. The
                    // seam is set AFTER start() (start() owns the channel's on_error); a clean
                    // tear_down sets m_torn_down first, so it does not fire here.
                    requester->on_transport_drop(
                            [this]
                            {
                                ++drops_seen;
                                driver->on_channel_dropped();
                            });
                });

        if(listen_first)
        {
            transport.listen({"tcp", "127.0.0.1:0"});
            port = transport.port();
        }
        else
        {
            port = closed_port;
        }
        driver.emplace(transport, io, cfg,
                       pio::endpoint{"tcp", "127.0.0.1:" + std::to_string(port)}, k_seed);
        // The driver no longer self-wires the transport's dial-failure callback (a
        // shared transport's single callback cannot belong to one of many drivers);
        // the owner routes a failure to its sole driver.
        transport.on_dial_failed([this](const pio::endpoint &, pio::io_error)
                                 { driver->notify_dial_failed(); });
        driver->on_redial(
                [this]
                {
                    if(requester)
                        requester->tear_down();
                });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(50))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    bool both_complete() const
    {
        return requester && responder && requester->is_complete() && responder->is_complete();
    }
};

reconnect_config fast_cfg()
{
    return reconnect_config{std::chrono::milliseconds(5), std::chrono::milliseconds(50),
                            std::nullopt, std::nullopt};
}

}

TEST_CASE("asio reconnect: an established session whose channel drops re-dials and re-handshakes "
          "over real TCP",
          "[integration][reconnect][asio]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tcp_reconnect h(fast_cfg(), /*listen_first=*/true);
        h.driver->start();
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        const auto first_epoch = h.requester->session_id();

        // Drop the established connection: close the accepted (server) socket. The
        // dialer's read loop surfaces connection_reset/broken_pipe → on_error → the
        // driver re-dials. (A clean tear_down is NOT used — this is a real transport drop.)
        h.resp_ctx.channel->socket().close();
        h.pump_until([&] { return h.drops_seen >= 1; });
        REQUIRE(h.drops_seen >= 1);
        REQUIRE(h.driver->attempt_count() >= 1);

        // The re-dial finds the still-up listener and re-handshakes to a fresh epoch.
        h.pump_until([&] { return h.both_complete() && h.requester->session_id() != first_epoch; });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        REQUIRE(h.requester->session_id() != first_epoch);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("asio reconnect: a dial to a closed port re-dials until a listener appears, then "
          "completes over real TCP",
          "[integration][reconnect][asio]")
{
    constexpr int k_iterations = 30;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // Bind a listener to learn a free port, then stop it so the port is CLOSED.
        ::asio::io_context    probe_io;
        pasio::asio_transport probe{probe_io};
        probe.listen({"tcp", "127.0.0.1:0"});
        const auto port = probe.port();
        probe.close(); // the port is now closed → a dial there is refused

        tcp_reconnect h(fast_cfg(), /*listen_first=*/false, port);
        h.driver->start();
        // The initial dial is refused; the driver schedules a re-dial (attempt advances).
        h.pump_until([&] { return h.driver->attempt_count() >= 1; });
        REQUIRE(h.driver->attempt_count() >= 1);
        REQUIRE(!h.driver->is_surrendered());
        REQUIRE(!h.both_complete());

        // Bring the endpoint up on the SAME port: a subsequent re-dial connects and completes.
        h.transport.listen({"tcp", "127.0.0.1:" + std::to_string(port)});
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

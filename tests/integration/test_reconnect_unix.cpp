// Gated real-AF_UNIX reconnect oracle over a local socket. The reconnect driver
// drives the dial-retry cycle on a real steady_timer: an established session whose
// channel drops (the socket is closed on the accepted end, surfacing a transport
// error on the dialer) tears down, backs off, re-dials through unix_transport::dial,
// and re-handshakes to a fresh epoch; a dial to a path with NO socket file
// (ENOENT/connection_refused) re-dials until a listener appears. The deterministic
// surrender / ceiling / backoff sweeps live on the inproc virtual clock (the
// sibling oracle, transport-agnostic); this leg validates the same-host cold-start
// demand resurrection and the established-drop re-dial behave on the real
// local-stream socket. The full scenario loops in-body and the ctest invocation is
// re-run >=3 process runs (a live-networking claim is never made from one run).
// Links plexus::inproc for the forwarder framing path.

#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/unix_channel.h"

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

#include <unistd.h>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using pio::handshake_fsm_config;
using pio::reconnect_config;
using session       = pio::peer_session<pasio::unix_policy>;
using msg_forwarder = pio::message_forwarder<pasio::unix_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::unix_policy>;
using driver_t =
        pio::reconnect<pasio::unix_policy, pasio::unix_transport, std::chrono::steady_clock>;

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

// A per-instance owner-only temp directory + a SHORT socket path within it.
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char        tmpl[] = "/tmp/pxu-XXXXXX";
        const char *made   = ::mkdtemp(tmpl);
        dir                = made ? made : "";
        path               = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// One reconnecting dialer over a real AF_UNIX socket. The driver re-dials the path
// after a drop or a refused cold-start. The dialer channel's on_error routes the
// transport drop to the driver (NOT a clean close); on_redial tears the dead
// requester down and carries its epoch forward so the next completion mints a
// strictly later epoch. The accepted (responder) end is rebuilt fresh per accept.
// Members are ordered sock/io/transport BEFORE the channels so destruction unwinds
// channels before the io_context.
struct unix_reconnect
{
    temp_sock             sock;
    ::asio::io_context    io;
    pasio::unix_transport transport{io};

    msg_forwarder req_messages{};
    msg_forwarder resp_messages{};
    rpc_forwarder req_procedures{io, k_long_timeout};
    rpc_forwarder resp_procedures{io, k_long_timeout};

    plexus::io::peer_context<pasio::unix_policy> req_ctx;
    plexus::io::peer_context<pasio::unix_policy> resp_ctx;
    std::optional<driver_t>                      driver;
    std::optional<session>                       requester;
    std::optional<session>                       responder;

    int drops_seen{0};

    // listen_first: bind the listener BEFORE the driver so the driver dials the live
    // path. When false the socket file is absent, so the initial dial is refused
    // (ENOENT) and the listener is brought up later — the same-host cold-start race.
    unix_reconnect(const reconnect_config &cfg, bool listen_first)
    {
        transport.on_accepted(
                [this](std::unique_ptr<pasio::unix_channel> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, io, make_cfg(0x01), k_long_timeout, resp_messages,
                                      resp_procedures, true);
                    responder->start();
                });
        transport.on_dialed(
                [this](std::unique_ptr<pasio::unix_channel> ch, const pio::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, io, make_cfg(0x02), k_long_timeout, req_messages,
                                      req_procedures, false);
                    requester->start();
                    // Route a transport DROP — not a clean close — to the driver through the
                    // session's production drop seam, set AFTER start() (start() owns the
                    // channel's on_error); a clean tear_down sets m_torn_down first.
                    requester->on_transport_drop(
                            [this]
                            {
                                ++drops_seen;
                                driver->on_channel_dropped();
                            });
                });

        if(listen_first)
            transport.listen({"unix", sock.path});

        driver.emplace(transport, io, cfg, pio::endpoint{"unix", sock.path}, k_seed);
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

TEST_CASE("unix reconnect: an established session whose channel drops re-dials and re-handshakes "
          "over real AF_UNIX",
          "[integration][reconnect][unix]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        unix_reconnect h(fast_cfg(), /*listen_first=*/true);
        h.driver->start();
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        const auto first_epoch = h.requester->session_id();

        // Drop the established connection: close the accepted (server) socket. The
        // dialer's read loop surfaces a transport error -> on_error -> the driver
        // re-dials. (A clean tear_down is NOT used — this is a real transport drop.)
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

TEST_CASE("unix reconnect: a cold-start dial to a missing socket re-dials until a listener "
          "appears, then completes over real AF_UNIX",
          "[integration][reconnect][unix]")
{
    constexpr int k_iterations = 30;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // No socket file bound yet: the initial dial is refused (ENOENT mapped to a
        // refused/connection error), the driver schedules a re-dial (the same-host
        // demand cold-start race — the demand must be resurrected, not dropped).
        unix_reconnect h(fast_cfg(), /*listen_first=*/false);
        h.driver->start();
        h.pump_until([&] { return h.driver->attempt_count() >= 1; });
        REQUIRE(h.driver->attempt_count() >= 1);
        REQUIRE(!h.driver->is_surrendered());
        REQUIRE(!h.both_complete());

        // Bring the endpoint up on the SAME path: a subsequent re-dial connects and completes.
        h.transport.listen({"unix", h.sock.path});
        h.pump_until([&] { return h.both_complete(); });
        REQUIRE(h.both_complete());
        REQUIRE(h.requester->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

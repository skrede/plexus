// The handshake-ARQ-over-loss leg: a UDP session cannot establish over a lossy link
// without retransmitting the handshake, so this proves the fixed-ladder ARQ
// (250/500/1000ms x3, here a compressed ladder for a fast deterministic run)
// re-sends and STILL establishes when the first handshake datagrams are dropped.
//
// The loss is injected by a deterministic UDP relay fixture (lossy_relay): the client
// dials the relay, the relay forwards datagrams to the real server (and replies back),
// DROPPING the first `drop_count` datagrams it sees in each direction-agnostic stream.
// This relay is the SAME reusable loss-injection seam a later reliable-data-ARQ leg
// composes — a wrapping forwarder, not a one-off. Each behavioral path loops in-body
// and the ctest invocation is re-run across >=3 process runs for cross-process
// reproducibility (a timing/transport claim is never made from one run).
//
// Covered:
//   * establish-under-loss: drop the first N handshake datagrams -> the ARQ retransmit
//     still lands the session (on_dialed fires).
//   * exhaustion: drop EVERY datagram -> the ARQ surrenders after 3 attempts and
//     surfaces on_dial_failed(timed_out), not a silent hang.
//   * the ARQ component directly: the retransmit ladder fires and a paired frame
//     cancels it; single-owner teardown cancels a pending timer cleanly.

#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/io/detail/udp_handshake_arq.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

namespace {

using ms = std::chrono::milliseconds;

// A compressed ladder so the loss legs run fast while exercising the SAME
// retransmit/surrender mechanics as the production 250/500/1000 ladder.
constexpr pasio::udp_transport::arq_type::schedule fast_ladder{ms{20}, ms{40}, ms{80}};

// A deterministic loss-injecting UDP relay between the dialing client and the real
// server. Forwards each datagram both ways but drops the first `drop_count` it sees,
// so the dialer's ARQ must retransmit to get a handshake through. The reusable
// loss-injection fixture (a wrapping forwarder).
struct lossy_relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;     // faces the client
    ::asio::ip::udp::socket back;      // faces the server
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;        // learned from the first client datagram
    ::asio::ip::udp::endpoint from;             // scratch for the active recv
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
    int drop_count;
    int dropped{0};

    lossy_relay(::asio::io_context &ctx, std::uint16_t server_port, int drops)
        : io(ctx)
        , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
        , drop_count(drops)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return;
                client_ep = from;
                if(!maybe_drop())
                    back.send_to(::asio::buffer(front_buf.data(), n), server_ep);
                recv_front();
            });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return;
                if(!maybe_drop() && client_ep.port() != 0)
                    front.send_to(::asio::buffer(back_buf.data(), n), client_ep);
                recv_back();
            });
    }

    bool maybe_drop()
    {
        if(dropped < drop_count)
        {
            ++dropped;
            return true;
        }
        return false;
    }
};

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{4000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

TEST_CASE("udp handshake_arq: the session establishes under injected loss via retransmit", "[udp][handshake]")
{
    constexpr int k_iterations = 30;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_ladder};

        bool accepted = false;
        bool dialed = false;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel>) { accepted = true; });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        // Drop the first datagram each way: the dialer's first hs_request (and/or the
        // first hs_response) is lost, so the ARQ must retransmit to establish.
        lossy_relay relay{io, server.port(), /*drops=*/1};

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel>, const plexus::io::endpoint &) { dialed = true; });
        client.dial({"udp", "127.0.0.1:" + std::to_string(relay.port())});

        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed);
        REQUIRE(accepted);
        REQUIRE(relay.dropped >= 1);     // a datagram WAS dropped — the path was genuinely lossy
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp handshake_arq: dropping every datagram surfaces a handshake-timeout abort", "[udp][handshake]")
{
    constexpr int k_iterations = 10;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_ladder};

        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        lossy_relay relay{io, server.port(), /*drops=*/10000};   // drop everything

        std::optional<plexus::io::io_error> dial_fail;
        bool dialed = false;
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel>, const plexus::io::endpoint &) { dialed = true; });
        client.on_dial_failed([&](const plexus::io::endpoint &, plexus::io::io_error e) { dial_fail = e; });
        client.dial({"udp", "127.0.0.1:" + std::to_string(relay.port())});

        pump_until(io, [&] { return dial_fail.has_value(); });
        REQUIRE_FALSE(dialed);
        REQUIRE(dial_fail.has_value());
        REQUIRE(*dial_fail == plexus::io::io_error::timed_out);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp handshake_arq component: the ladder retransmits and a paired frame cancels it", "[udp][handshake]")
{
    using arq_t = pio::detail::udp_handshake_arq<pasio::udp_policy>;

    SECTION("a paired frame mid-ladder establishes and stops further transmits")
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};

        int transmits = 0;
        bool established = false;
        bool timed_out = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_established([&] { established = true; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start();                                     // transmit #1 fires immediately
        REQUIRE(transmits == 1);
        pump_until(io, [&] { return transmits >= 2; });  // the 20ms timer fires transmit #2
        REQUIRE(transmits >= 2);

        arq.on_paired_frame();                           // the response arrives
        REQUIRE(established);
        int at_pairing = transmits;
        pump_until(io, [&] { return false; }, ms{120});  // let the would-be ladder elapse
        REQUIRE(transmits == at_pairing);                // no further transmit after resolution
        REQUIRE_FALSE(timed_out);
    }

    SECTION("exhausting the ladder surfaces a timeout abort")
    {
        ::asio::io_context io;
        arq_t arq{io, fast_ladder};

        int transmits = 0;
        bool timed_out = false;
        arq.on_transmit([&] { ++transmits; });
        arq.on_timeout([&] { timed_out = true; });

        arq.start();
        pump_until(io, [&] { return timed_out; });
        REQUIRE(timed_out);
        REQUIRE(transmits == 3);                         // the three ladder attempts, then surrender
    }

    SECTION("single-owner teardown cancels a pending timer cleanly")
    {
        ::asio::io_context io;
        {
            arq_t arq{io, fast_ladder};
            arq.on_transmit([] {});
            arq.start();                                 // a timer is pending
            arq.cancel();                                // the owner tears it down
            REQUIRE(arq.resolved());
        }
        io.poll();                                       // a cancelled timer must not fire on a dead ARQ
        SUCCEED();
    }
}

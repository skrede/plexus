// The outbound-send-lifetime gate: the shared udp_server owns each in-flight datagram
// across its async_send_to (a serial owned queue), so a BURST of sends issued in ONE
// turn — without draining the io_context between them — each transmits its OWN bytes,
// not a neighbor's reused scratch. This is the reproducibility proof for the buffer-
// lifetime defect: against a sink that referenced a caller-owned, immediately-reused
// scratch buffer across the async op, every leg below would transmit corrupted or
// torn bytes (the LAST scratch content), so the test would FAIL. Three legs:
//   * best_effort burst: N datagrams of DISTINCT, size-varying payloads queued in one
//     turn (no drain between sends) all arrive intact and in publish order.
//   * two-peer overlap: two channels over the SAME server each send in the same turn —
//     neither peer's in-flight datagram is overwritten by the other's.
//   * reliable retransmit vs fresh send: a lossy relay forces an ARQ retransmit to
//     interleave with fresh submits; every reliable frame still arrives in order.
// Looped 100x in-body; the ctest invocation is re-run across >=3 process runs (a
// transport/timing claim is never made from a single run).

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <deque>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

inline pio::detail::udp_arq_config fast_arq()
{
    return pio::detail::udp_arq_config{.window         = 64,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{80},
                                       .max_retransmit = 12};
}

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{6000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

// A listen+dial best_effort pair on one io_context (handshake established).
struct pair_fixture
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::vector<std::string>            received;

    pair_fixture()
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b)
                                      { received.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [this] { return server.port() != 0; });

        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [this] { return dialed && accepted; });
    }
};

}

TEST_CASE("udp burst: N best_effort datagrams queued in one turn each arrive intact, in order",
          "[udp][burst][reproducibility]")
{
    constexpr int k_iterations = 100;
    constexpr int k_burst      = 8;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        pair_fixture h;
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        // Issue the whole burst in ONE turn: NO io.poll() between sends, so every
        // datagram's bytes are still queued when the next send overwrites the channel's
        // send scratch. Distinct, size-varying payloads so a reused-scratch sink would
        // transmit the LAST payload (or torn bytes / a reallocated buffer) for the earlier
        // legs — the receiver would then NOT see each distinct payload exactly once.
        std::vector<std::string> sent;
        for(int i = 0; i < k_burst; ++i)
        {
            std::string p = "burst-" + std::to_string(iter) + "-" + std::to_string(i) +
                    std::string(static_cast<std::size_t>(i) * 7, 'x'); // size varies per leg
            sent.push_back(p);
            h.dialed->send(bytes_of(p)); // queued; the scratch is reused on the next iteration
        }

        pump_until(h.io, [&] { return h.received.size() == sent.size(); });
        REQUIRE(h.received == sent); // each distinct payload, intact, in publish order
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp burst: two channels over one server each send in the same turn without overlap",
          "[udp][burst][reproducibility]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // One listening server, TWO dialing clients (each its own transport/server), so
        // two DISTINCT accepted channels share the listening server's ONE outbound socket
        // for their handshake responses and any sends. The interest is the listening
        // server's send path: it must keep each in-flight datagram alive.
        ::asio::io_context   io;
        pasio::udp_transport server{io};
        pasio::udp_transport client_a{io, pasio::udp_channel::default_max_payload, fast_hs};
        pasio::udp_transport client_b{io, pasio::udp_channel::default_max_payload, fast_hs};

        std::vector<std::unique_ptr<pasio::udp_channel>> accepted;
        std::unique_ptr<pasio::udp_channel>              dia_a, dia_b;
        std::vector<std::string> dialer_seen; // every payload the two dialers receive
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch)
                           { accepted.push_back(std::move(ch)); });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        const std::string ep = "udp", host = "127.0.0.1:" + std::to_string(server.port());
        client_a.on_dialed(
                [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                {
                    dia_a = std::move(ch);
                    dia_a->on_data([&](std::span<const std::byte> b)
                                   { dialer_seen.push_back(str_of(b)); });
                });
        client_b.on_dialed(
                [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                {
                    dia_b = std::move(ch);
                    dia_b->on_data([&](std::span<const std::byte> b)
                                   { dialer_seen.push_back(str_of(b)); });
                });
        client_a.dial({ep, host});
        client_b.dial({ep, host});
        pump_until(io, [&] { return accepted.size() == 2 && dia_a && dia_b; });
        REQUIRE(accepted.size() == 2);

        // The two accepted channels (sharing the listening server's ONE outbound socket)
        // each send to their OWN dialer in the same turn, so the server's outbound queue
        // holds two distinct in-flight datagrams at once. Distinct, size-varying payloads:
        // a reused scratch would cross them or transmit torn bytes for the earlier leg.
        const std::string pa = "to-peer-a-" + std::to_string(iter);
        const std::string pb = "to-peer-b-" + std::to_string(iter) + "-longer-tail";
        accepted[0]->send(bytes_of(pa));
        accepted[1]->send(bytes_of(pb));

        pump_until(io, [&] { return dialer_seen.size() == 2; });
        // Both payloads arrive intact across the two dialers — neither overwritten by the
        // other's in-flight datagram (order across peers is not guaranteed; the SET is).
        std::sort(dialer_seen.begin(), dialer_seen.end());
        std::vector<std::string> expect{pa, pb};
        std::sort(expect.begin(), expect.end());
        REQUIRE(dialer_seen == expect);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

namespace {

enum class action
{
    pass,
    drop
};

// A drop-scripting relay (acks + handshake always pass; a scripted data seq drops once
// then retransmits) — the same shape as the reproducibility relay, reused here to force
// a retransmit to interleave with fresh sends.
struct relay
{
    ::asio::io_context         &io;
    ::asio::ip::udp::socket     front;
    ::asio::ip::udp::socket     back;
    ::asio::ip::udp::endpoint   server_ep;
    ::asio::ip::udp::endpoint   client_ep;
    ::asio::ip::udp::endpoint   from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
    std::deque<action>          data_script;

    relay(::asio::io_context &ctx, std::uint16_t server_port)
            : io(ctx)
            , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    [[nodiscard]] static bool is_data(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        return dec && dec->kind == wire::udp_envelope_kind::reliable_arq &&
                wire::peek_udp_arq_kind(dec->frame) == wire::udp_arq_kind::segment;
    }

    void to_server(std::span<const std::byte> dg)
    {
        std::vector<std::byte> copy(dg.begin(), dg.end());
        back.send_to(::asio::buffer(copy.data(), copy.size()), server_ep);
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
                                 [this](std::error_code ec, std::size_t n)
                                 {
                                     if(ec)
                                         return;
                                     client_ep = from;
                                     std::span<const std::byte> dg{front_buf.data(), n};
                                     if(is_data(dg))
                                     {
                                         action a = action::pass;
                                         if(!data_script.empty())
                                         {
                                             a = data_script.front();
                                             data_script.pop_front();
                                         }
                                         if(a == action::pass)
                                             to_server(dg);
                                     }
                                     else
                                         to_server(dg);
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
                                    if(client_ep.port() != 0)
                                        front.send_to(::asio::buffer(back_buf.data(), n),
                                                      client_ep);
                                    recv_back();
                                });
    }
};

}

TEST_CASE("udp burst: a reliable retransmit interleaved with fresh sends delivers in order",
          "[udp][burst][reliable][reproducibility]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs,
                                    fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string>            delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        relay link{io, server.port()};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);

        // Drop the 1st segment once: its RTO-driven retransmit then races the fresh sends
        // of the later segments, so a retransmit's on_transmit shares the send path with a
        // fresh submit's on_transmit in overlapping turns — the exact send-buffer aliasing
        // the owned outbound queue defends against.
        link.data_script = {action::drop};

        std::vector<std::string> sent;
        for(int i = 0; i < 6; ++i)
        {
            const std::string p = "rtx-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p)); // reliable mode: a burst of submits in one turn
        }
        pump_until(io, [&] { return delivered.size() == sent.size(); });
        REQUIRE(delivered == sent); // in order, complete, retransmit-interleaved
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

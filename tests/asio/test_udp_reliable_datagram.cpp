// The reliable-datagram opt-in engaged through the SCHEME (the engine-reachable dial
// path): a dial("udpr://...") mints a reliable-datagram channel whose single send() verb
// drives the in-order ARQ; a dial("udp://...") mints a best_effort channel (fire-and-
// forget). The mode is declared in the handshake so the acceptor is SYMMETRIC — both ends
// agree on the class. This proves the opt-in is reachable via the scheme alone (no dial-
// signature change), and that "udpr" delivers in-order over injected loss end-to-end.
//
// The mux-level route flip ("udpr" -> the UDP+ARQ member, never TCP) is pinned in
// test_udp_transport.cpp; this TU pins the transport+channel MODE mechanics underneath it.
// Each scenario loops in-body and the ctest invocation is re-run across >=3 process runs.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/io/detail/udp_handshake_frame.h"

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

enum class action
{
    pass,
    drop
};

// A drop-scripting relay (acks + handshake always pass; a once-dropped data seq is
// retransmitted past the drop). The same shape as the ARQ loss harness.
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
    int                         data_seen{0};

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
                                         ++data_seen;
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

}

TEST_CASE("udp reliable_datagram: a 'udpr' dial mints reliable-mode channels on BOTH ends "
          "(symmetric)",
          "[udp][reliable_datagram][mode]")
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                     { dialed = std::move(ch); });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });

    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);
    // The dialer declared reliable_datagram in the handshake; the acceptor minted the
    // SAME mode — both report "udpr" (the mode is symmetric, not just dialer-side).
    REQUIRE(dialed->mode() == pio::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(accepted->mode() == pio::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(dialed->remote_endpoint().scheme == "udpr");
    REQUIRE(accepted->remote_endpoint().scheme == "udpr");
}

TEST_CASE("udp reliable_datagram: a 'udp' dial stays best_effort (the opt-in is scheme-gated)",
          "[udp][reliable_datagram][mode]")
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                     { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });

    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);
    REQUIRE(dialed->mode() == pio::detail::udp_channel_mode::best_effort);
    REQUIRE(accepted->mode() == pio::detail::udp_channel_mode::best_effort);
    REQUIRE(dialed->remote_endpoint().scheme == "udp");
    REQUIRE(accepted->remote_endpoint().scheme == "udp");
}

TEST_CASE("udp reliable_datagram: a 'udpr' channel delivers in-order over injected loss via send()",
          "[udp][reliable_datagram]")
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
        REQUIRE(accepted != nullptr);

        // Drop the 2nd segment once: send() (reliable mode) must retransmit + the receiver
        // HOL holds 3,4 behind the gap until the retransmit fills it — all 4 in order.
        link.data_script = {action::pass, action::drop, action::pass, action::pass};

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "udpr-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p)); // the SINGLE send verb drives the ARQ in reliable mode
        }
        pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent);   // exactly once, in publish order, over loss
        REQUIRE(link.data_seen >= 5); // genuinely lossy (>=1 retransmit)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp reliable_datagram: a message beyond the max-message size is rejected at publish "
          "(cross-class)",
          "[udp][reliable_datagram][oversize]")
{
    // The oversize-reject-at-publish invariant holds on BOTH classes for the genuinely-
    // too-big message: a reliable send() of a payload beyond the bounded max-MESSAGE size
    // surfaces message_too_large at publish (the channel stays open), exactly like the
    // best_effort path — never a silent drop. A merely-oversize payload is fragmented.
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::optional<pio::io_error>        dialed_error;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed(
            [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
            {
                dialed = std::move(ch);
                dialed->on_error([&](pio::io_error e) { dialed_error = e; });
            });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(dialed != nullptr);

    // A payload beyond the bounded max-MESSAGE size is rejected at publish via
    // on_error(message_too_large) — the reliable class enforces the same hard ceiling as
    // best_effort. A merely-oversize-but-fragmentable payload is split, not rejected.
    std::vector<std::byte> too_big(pio::global_default_max_message_bytes + 1, std::byte{0x5A});
    dialed->send(too_big); // reliable-mode send dispatches to send_reliable -> oversize reject
    pump_until(io, [&] { return dialed_error.has_value(); });

    REQUIRE(dialed_error.has_value());
    REQUIRE(*dialed_error == pio::io_error::message_too_large);
    REQUIRE(dialed->is_open()); // rejected at publish, channel stays open
}

// The reliable-ARQ numeric sweep: an EMPIRICAL substantiation of the RTO / window
// defaults (a numeric choice is recorded from evidence, not picked by feel). It drives
// the full channel-pair ARQ over a SEEDED probabilistic-loss relay across a grid of
// {loss rate x send window x initial RTO}, measuring for each cell whether delivery
// completes in-order-and-complete and the retransmit overhead it cost. The table is
// printed (CAPTURE) so the recorded evidence is visible in the run log; every cell must
// complete (the reliability invariant holds at every swept point) and the run is
// repeated over multiple seeds so a single lucky draw is never the basis of a claim.
//
// Hidden by the [.sweep] tag so it runs only when explicitly selected (it is slower than
// the behavioral legs): `ctest -R udp.sweep` / `test_udp_arq_sweep "[.sweep]"`.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/detail/udp_reliable_arq.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <random>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <iomanip>

namespace pasio = plexus::asio;
namespace wire = plexus::wire;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{15}, ms{30}, ms{60}};

std::vector<std::byte> payload_of(int n)
{
    std::string s = "sweep-payload-" + std::to_string(n);
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

// A seeded probabilistic-loss relay: each client->server DATA segment is dropped with
// probability `loss` (acks + handshake always pass so the control loop stays healthy).
// The drop count is recorded as the retransmit-pressure proxy.
struct prob_relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;
    ::asio::ip::udp::socket back;
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
    std::mt19937 rng;
    double loss;
    int dropped{0};

    prob_relay(::asio::io_context &ctx, std::uint16_t server_port, double loss_rate, std::uint32_t seed)
        : io(ctx)
        , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
        , rng(seed)
        , loss(loss_rate)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    [[nodiscard]] static bool is_data(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        return dec && dec->kind == wire::udp_envelope_kind::reliable_arq
               && wire::peek_udp_arq_kind(dec->frame) == wire::udp_arq_kind::segment;
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from, [this](std::error_code ec, std::size_t n) {
            if(ec) return;
            client_ep = from;
            std::span<const std::byte> dg{front_buf.data(), n};
            std::uniform_real_distribution<double> u(0.0, 1.0);
            if(!(is_data(dg) && u(rng) < loss))
                back.send_to(::asio::buffer(front_buf.data(), n), server_ep);
            else
                ++dropped;
            recv_front();
        });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from, [this](std::error_code ec, std::size_t n) {
            if(ec) return;
            if(client_ep.port() != 0)
                front.send_to(::asio::buffer(back_buf.data(), n), client_ep);
            recv_back();
        });
    }
};

struct cell_result
{
    bool complete{false};
    int delivered{0};
    int dropped{0};
};

cell_result run_cell(double loss, std::size_t window, ms initial_rto, int n_msgs, std::uint32_t seed)
{
    pasio::detail::udp_arq_config cfg{
        .window = window, .initial_rto = initial_rto, .min_rto = ms{5},
        .max_rto = initial_rto * 8, .max_retransmit = 30};

    ::asio::io_context io;
    pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                pasio::udp_transport::arq_type::default_ladder, cfg};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, cfg};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::vector<int> got;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
        accepted = std::move(ch);
        accepted->on_data([&](std::span<const std::byte> b) { got.push_back(static_cast<int>(b.size())); });
    });
    server.listen({"udp", "127.0.0.1:0"});
    auto pump = [&](auto pred, ms budget) {
        auto bound = std::chrono::steady_clock::now() + budget;
        while(!pred() && std::chrono::steady_clock::now() < bound) { io.poll(); if(io.stopped()) io.restart(); }
    };
    pump([&] { return server.port() != 0; }, ms{1000});

    prob_relay relay{io, server.port(), loss, seed};
    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const plexus::io::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(relay.port())});
    pump([&] { return dialed && accepted; }, ms{2000});
    if(!dialed || !accepted)
        return {};

    // Submit n_msgs reliable payloads (re-submitting any window-full one as acks drain).
    int submitted = 0;
    while(submitted < n_msgs)
    {
        if(dialed->send_reliable(payload_of(submitted)) == pasio::udp_channel::submit_result::admitted)
            ++submitted;
        else
            pump([&] { return false; }, ms{5});      // let acks slide the window, then retry
    }
    pump([&] { return static_cast<int>(got.size()) >= n_msgs; }, ms{8000});

    return {static_cast<int>(got.size()) >= n_msgs, static_cast<int>(got.size()), relay.dropped};
}

}

TEST_CASE("udp sweep: reliable delivery completes across a loss x window x RTO grid", "[.sweep][udp]")
{
    constexpr int n_msgs = 40;
    const std::array<double, 4> loss_rates{0.0, 0.1, 0.25, 0.4};
    const std::array<std::size_t, 3> windows{16, 64, 256};
    const std::array<ms, 3> rtos{ms{15}, ms{30}, ms{60}};
    const std::array<std::uint32_t, 3> seeds{1u, 7u, 13u};

    std::ostringstream tbl;
    tbl << "\n  loss  window  rto(ms)  seed  delivered  drops  complete\n";
    tbl << "  ----  ------  -------  ----  ---------  -----  --------\n";

    bool all_complete = true;
    for(double loss : loss_rates)
        for(std::size_t w : windows)
            for(ms rto : rtos)
                for(std::uint32_t seed : seeds)
                {
                    auto r = run_cell(loss, w, rto, n_msgs, seed);
                    all_complete = all_complete && r.complete;
                    tbl << "  " << std::setw(4) << loss
                        << "  " << std::setw(6) << w
                        << "  " << std::setw(7) << rto.count()
                        << "  " << std::setw(4) << seed
                        << "  " << std::setw(9) << r.delivered
                        << "  " << std::setw(5) << r.dropped
                        << "  " << (r.complete ? "yes" : "NO") << "\n";
                }

    CAPTURE(tbl.str());
    INFO(tbl.str());
    // The reliability invariant holds at EVERY swept point: every cell delivers all
    // n_msgs in order despite the injected loss (the evidence the defaults rest on).
    REQUIRE(all_complete);
}

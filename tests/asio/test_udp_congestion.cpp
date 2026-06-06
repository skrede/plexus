// The congestion knobs (D-05) over the reliable-datagram ARQ: when a reliable send
// window is FULL, the per-channel congestion mode decides the publisher's fate.
//   * block (the safe reliable default): the publish is back-pressured into a BOUNDED
//     publish-side queue drained by the next ack — publish() never blocks, the io_context
//     is NEVER stalled, and EVERY frame eventually arrives IN ORDER (the reliable
//     guarantee is preserved over a full window, even over loss).
//   * drop: the window-full frame is shed at the publisher (the documented opt-out of the
//     guarantee) — earlier frames still arrive, the shed one does not.
//
// The load-bearing negative proof: congestion=block does NOT block the io_context. While
// one channel's reliable window is saturated and back-pressured, a CONCURRENT best_effort
// flow on another peer keeps flowing — the loop is servicing other work, not deadlocked
// inside a publish (Pitfall 5 / T-15-12). Each scenario loops in-body and the ctest
// invocation is re-run across >=3 process runs (a transport/timing claim is never made
// from a single run).

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/detail/udp_reliable_arq.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/io/congestion.h"

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
namespace wire = plexus::wire;
namespace pio = plexus::io;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A compressed ARQ config with a SMALL window so a modest publish burst overruns it and
// exercises the congestion path quickly. Bound at construction (the same structural seam
// production uses) — not a mutable setter.
inline pasio::detail::udp_arq_config small_window_arq(std::size_t window)
{
    return pasio::detail::udp_arq_config{
        .window = window, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{120}, .max_retransmit = 12};
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

enum class action { pass, drop };

// A programmable relay between a dialing client and a real server: it forwards every
// datagram both ways and scripts per-data-segment drops so the test controls the loss
// (acks + handshake always pass). The retransmit of a once-dropped seq passes (the drop
// is consumed once), so the ARQ recovers under its RTO.
struct relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;
    ::asio::ip::udp::socket back;
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
    std::deque<action> data_script;
    int data_seen{0};

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
        return dec && dec->kind == wire::udp_envelope_kind::reliable_arq
               && wire::peek_udp_arq_kind(dec->frame) == wire::udp_arq_kind::segment;
    }

    void to_server(std::span<const std::byte> dg)
    {
        std::vector<std::byte> copy(dg.begin(), dg.end());
        back.send_to(::asio::buffer(copy.data(), copy.size()), server_ep);
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from, [this](std::error_code ec, std::size_t n) {
            if(ec) return;
            client_ep = from;
            std::span<const std::byte> dg{front_buf.data(), n};
            if(is_data(dg))
            {
                ++data_seen;
                action a = action::pass;
                if(!data_script.empty()) { a = data_script.front(); data_script.pop_front(); }
                if(a == action::pass) to_server(dg);
            }
            else to_server(dg);
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

template <typename Pred>
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

// A reliable-datagram channel pair behind a programmable relay, handshake established.
// The channel mode is reliable_datagram (its send() drives the ARQ); the congestion mode
// and window depth are configurable so the congestion path is exercised deterministically.
struct fixture
{
    ::asio::io_context io;
    pasio::udp_transport server;
    pasio::udp_transport client;
    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;
    std::unique_ptr<relay> link;
    std::vector<std::string> delivered;

    fixture(pio::congestion cong, std::size_t window)
        : server(io, pasio::udp_channel::default_max_payload,
                 pasio::udp_transport::arq_type::default_ladder, small_window_arq(window), cong)
        , client(io, pasio::udp_channel::default_max_payload, fast_hs, small_window_arq(window), cong)
    {
        server.on_accepted([this](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([this](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [this] { return server.port() != 0; });

        link = std::make_unique<relay>(io, server.port());
        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link->port())});   // reliable-datagram mode
        pump_until(io, [this] { return dialed && accepted; });
    }
};

}

TEST_CASE("udp congestion block: a full window back-pressures and every frame arrives in order",
          "[udp][congestion]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f{pio::congestion::block, /*window=*/4};
        REQUIRE(f.dialed != nullptr);
        REQUIRE(f.accepted != nullptr);
        REQUIRE(f.dialed->congestion_mode() == pio::congestion::block);

        // Drop the 3rd segment once to keep the window full longer (the ARQ must
        // retransmit it before the base slides) — the back-pressure path is exercised
        // under genuine loss, not just a clean burst.
        f.link->data_script = {action::pass, action::pass, action::drop};

        // Publish 12 frames into a window of 4: 8 of them overrun the window and are
        // back-pressured into the bounded queue. send() (reliable mode) is non-blocking;
        // the acks drain the queue and ALL 12 must arrive in publish order.
        constexpr int n = 12;
        std::vector<std::string> sent;
        for(int i = 0; i < n; ++i)
        {
            const std::string p = "block-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            f.dialed->send(bytes_of(p));     // never blocks even when the window is full
        }
        // The queue must have absorbed the overrun (more than the window was published).
        REQUIRE(f.dialed->backpressured() > 0);

        pump_until(f.io, [&] { return f.delivered.size() == static_cast<std::size_t>(n); });
        REQUIRE(f.delivered == sent);                       // ALL arrive, in publish order
        REQUIRE(f.dialed->backpressured() == 0);            // the queue fully drained
        REQUIRE(f.dialed->dropped_count() == 0);            // block sheds NOTHING
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp congestion block does NOT stall the io_context: a concurrent flow keeps flowing",
          "[udp][congestion]")
{
    constexpr int k_iterations = 30;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The reliable channel pair on a TINY window (1) so it is back-pressured the
        // instant a 2nd frame is published. A SECOND, independent best_effort peer pair
        // shares the SAME io_context — its fire-and-forget flow must keep being serviced
        // while the reliable window is saturated (proof the loop is not blocked).
        fixture f{pio::congestion::block, /*window=*/1};
        REQUIRE(f.dialed != nullptr);

        // The concurrent best_effort pair (own transports on f.io).
        pasio::udp_transport be_server{f.io};
        pasio::udp_transport be_client{f.io, pasio::udp_channel::default_max_payload, fast_hs};
        std::unique_ptr<pasio::udp_channel> be_acc, be_dialed;
        std::optional<std::string> be_got;
        be_server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            be_acc = std::move(ch);
            be_acc->on_data([&](std::span<const std::byte> b) { be_got = str_of(b); });
        });
        be_server.listen({"udp", "127.0.0.1:0"});
        pump_until(f.io, [&] { return be_server.port() != 0; });
        be_client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { be_dialed = std::move(ch); });
        be_client.dial({"udp", "127.0.0.1:" + std::to_string(be_server.port())});
        pump_until(f.io, [&] { return be_dialed && be_acc; });
        REQUIRE(be_dialed != nullptr);

        // Saturate the reliable window: publish many frames into a window of 1, so all but
        // the first are back-pressured. publish() returns immediately (no block).
        for(int i = 0; i < 16; ++i)
            f.dialed->send(bytes_of("rel-" + std::to_string(iter) + "-" + std::to_string(i)));
        REQUIRE(f.dialed->backpressured() > 0);             // genuinely back-pressured

        // WHILE the reliable channel is saturated, the best_effort flow must complete —
        // if congestion=block had blocked the io_context, this would never be serviced.
        be_dialed->send(bytes_of("concurrent-alive"));
        pump_until(f.io, [&] { return be_got.has_value(); });
        REQUIRE(be_got.has_value());                        // the loop kept servicing other work
        REQUIRE(*be_got == "concurrent-alive");

        // And the reliable flow still completes (back-pressure delivers, never deadlocks).
        pump_until(f.io, [&] { return f.delivered.size() == 16; });
        REQUIRE(f.delivered.size() == 16);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp congestion drop: a window-full frame is shed at the publisher", "[udp][congestion]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // A window of 2, congestion=drop: publish 5 frames. The first 2 fill the window;
        // the next 3 (with no ack yet to slide it) are shed at the publisher. NO ack is
        // allowed to slide the window mid-burst (the relay holds acks back is unnecessary
        // — the burst is synchronous, so the window is full when 3,4,5 are published).
        fixture f{pio::congestion::drop, /*window=*/2};
        REQUIRE(f.dialed != nullptr);
        REQUIRE(f.dialed->congestion_mode() == pio::congestion::drop);

        constexpr int n = 5;
        for(int i = 0; i < n; ++i)
            f.dialed->send(bytes_of("drop-" + std::to_string(iter) + "-" + std::to_string(i)));

        // drop never enqueues — the backpressure queue stays empty; the overrun is shed.
        REQUIRE(f.dialed->backpressured() == 0);
        REQUIRE(f.dialed->dropped_count() == static_cast<std::size_t>(n - 2));   // 3 shed past the window of 2

        // The admitted (windowed) frames still deliver; the shed ones never arrive. Give
        // the path time to settle, then assert fewer than n were delivered (the guarantee
        // was opted out of for the shed segments).
        pump_until(f.io, [&] { return f.delivered.size() >= 2; });
        pump_until(f.io, [&] { return false; }, ms{60});
        REQUIRE(f.delivered.size() < static_cast<std::size_t>(n));   // the shed frames are gone
        REQUIRE(f.delivered.size() >= 2);                            // the windowed frames delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp congestion block: the bounded queue at its cap surfaces a would_block stall signal",
          "[udp][congestion]")
{
    // A standalone channel (no real peer, so NO acks slide the window) with a small window
    // AND a small backpressure cap: the window fills, then the queue fills, then the next
    // window-full publish has nowhere to go and surfaces would_block (the stall edge —
    // bounded, never unbounded growth, T-15-13). publish() stays non-blocking throughout.
    ::asio::io_context io;
    pasio::udp_server server{io};                 // unbound: send_to is a no-op sink, the window never drains
    constexpr std::size_t window = 2, cap = 3;
    pasio::udp_channel ch{io, server, ::asio::ip::udp::endpoint{::asio::ip::udp::v4(), 9},
                          pasio::udp_channel::default_max_payload, small_window_arq(window),
                          pio::congestion::block, cap, pasio::detail::udp_channel_mode::reliable_datagram};

    std::optional<pio::io_error> err;
    ch.on_error([&](pio::io_error e) { err = e; });

    // Fill the window (2) — admitted; fill the queue (3) — admitted into the queue; the
    // 6th publish overflows the cap -> would_block.
    for(std::size_t i = 0; i < window + cap; ++i)
        ch.send(bytes_of("q-" + std::to_string(i)));
    REQUIRE(ch.backpressured() == cap);           // the queue is at its cap, not growing past it
    REQUIRE_FALSE(err.has_value());               // no stall yet — the cap is not exceeded

    ch.send(bytes_of("overflow"));                // past window + cap -> the stall edge
    REQUIRE(err.has_value());
    REQUIRE(*err == pio::io_error::would_block);
    REQUIRE(ch.backpressured() == cap);           // still bounded at the cap (never grew)
    REQUIRE(ch.dropped_count() == 0);             // block does not shed — it stalls
}

TEST_CASE("udp congestion block: a sustained reliable load completes within a bounded budget (no freeze)",
          "[udp][congestion]")
{
    // A sustained burst far exceeding the window must complete (the node does not freeze):
    // a bounded time budget the test asserts is met. Over a window of 8, publish 200
    // frames under intermittent loss — the back-pressure queue + ack-drain must carry
    // them all to in-order delivery within the budget.
    fixture f{pio::congestion::block, /*window=*/8};
    REQUIRE(f.dialed != nullptr);

    // Drop every 7th segment once to keep retransmit pressure on while the queue drains.
    std::deque<action> script;
    for(int i = 0; i < 200; ++i)
        script.push_back((i % 7 == 6) ? action::drop : action::pass);
    f.link->data_script = script;

    constexpr int n = 200;
    std::vector<std::string> sent;
    for(int i = 0; i < n; ++i)
    {
        const std::string p = "sustained-" + std::to_string(i);
        sent.push_back(p);
        f.dialed->send(bytes_of(p));
    }

    const auto start = std::chrono::steady_clock::now();
    pump_until(f.io, [&] { return f.delivered.size() == static_cast<std::size_t>(n); }, ms{8000});
    const auto elapsed = std::chrono::steady_clock::now() - start;

    REQUIRE(f.delivered.size() == static_cast<std::size_t>(n));   // every frame arrived
    REQUIRE(f.delivered == sent);                                 // in publish order
    REQUIRE(f.dialed->backpressured() == 0);                      // queue fully drained
    REQUIRE(elapsed < std::chrono::seconds(8));                   // within the bounded budget (no freeze)
}

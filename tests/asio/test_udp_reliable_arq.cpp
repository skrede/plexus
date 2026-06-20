// over-limit: one cohesive selective-repeat ARQ-over-loss matrix; every cell drives the one
// shared programmable lossy-relay channel-pair fixture (the drop/reorder/duplicate scripting +
// the window/RTO config), so splitting the cells scatters that shared relay + ARQ fixture state.
// The reliable-data ARQ over loss: the load-bearing beyond-parity proof. A
// selective-repeat sliding window + a head-of-line reorder receiver + an adaptive RTO
// (Karn) must deliver every reliable payload EXACTLY ONCE and IN PUBLISH ORDER over a
// deterministically lossy/reordering/duplicating link. The loss is injected by a
// programmable UDP relay (an extension of the handshake-ARQ lossy_relay): the handshake
// and the acks always pass; the relay scripts per-data-segment actions (drop / reorder /
// duplicate) so the test controls exactly which segments are lost or shuffled and the
// ARQ retransmit/reorder is what restores order.
//
// Each scenario loops 100 iterations in-body and the ctest invocation is re-run across
// >=3 process runs (a transport/timing claim is NEVER made from a single run — the
// scenarios use a deliberately lossy path, so the proof is reproducibility, not luck).
//
// Covered:
//   (a) a single lost data segment is retransmitted and delivered in order;
//   (b) out-of-order arrival behind a gap is held then released in order (HOL e2e);
//   (c) a duplicated segment is deduped (delivered once);
//   (d) all-loss for one segment exhausts the retransmit cap -> a connection-fatal error.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/io/detail/udp_reliable_arq.h"

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
#include <functional>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A compressed ARQ config so the loss legs run fast while exercising the SAME
// selective-repeat/RTO/exhaustion mechanics as the swept production defaults: a small
// initial RTO and a low retransmit cap keep the exhaustion leg sub-second. NOT a
// mutable setter — bound at construction via the same structural seam production uses.
inline pio::detail::udp_arq_config fast_arq()
{
    return pio::detail::udp_arq_config{.window         = 64,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{80},
                                       .max_retransmit = 10};
}

// The exhaustion leg wants a LOW cap (and fast RTO) so the budget runs out sub-second.
inline pio::detail::udp_arq_config exhaust_arq()
{
    return pio::detail::udp_arq_config{.window         = 64,
                                       .initial_rto    = ms{15},
                                       .min_rto        = ms{5},
                                       .max_rto        = ms{40},
                                       .max_retransmit = 4};
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

// What the relay should do to one inbound data segment (kind=1, segment marker). The
// relay always passes handshakes and acks; only data segments are scripted.
enum class action
{
    pass,
    drop,
    duplicate,
    hold
};

// A programmable loss-injecting relay between a dialing client and a real server. It
// forwards every datagram both ways; for client->server DATA segments it consults a
// scripted action queue (drop one, duplicate one, hold-then-release to reorder).
struct relay
{
    ::asio::io_context         &io;
    ::asio::ip::udp::socket     front; // faces the client
    ::asio::ip::udp::socket     back;  // faces the server
    ::asio::ip::udp::endpoint   server_ep;
    ::asio::ip::udp::endpoint   client_ep;
    ::asio::ip::udp::endpoint   from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};

    std::deque<action>                  data_script; // consumed per client->server data segment
    std::vector<std::vector<std::byte>> held;        // held segments to release out of order
    std::vector<std::uint16_t>          held_seqs;   // seqs to keep holding (incl. retransmits)
    int                                 data_seen{0};

    [[nodiscard]] static std::uint16_t seq_of(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        return dec ? dec->seq : 0;
    }

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

    [[nodiscard]] static bool is_data_segment(std::span<const std::byte> dg)
    {
        auto dec = wire::unwrap_udp(dg);
        if(!dec || dec->kind != wire::udp_envelope_kind::reliable_arq)
            return false;
        auto k = wire::peek_udp_arq_kind(dec->frame);
        return k == wire::udp_arq_kind::segment;
    }

    void send_to_server(std::span<const std::byte> dg)
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
                                     if(is_data_segment(dg))
                                         handle_data(dg);
                                     else
                                         send_to_server(dg); // handshake + (client-side) acks pass
                                     recv_front();
                                 });
    }

    void handle_data(std::span<const std::byte> dg)
    {
        ++data_seen;
        const auto seq = seq_of(dg);
        // A retransmit of an already-held seq stays held (the gap is kept open until the
        // test releases it — otherwise a fast RTO would refill the gap on its own).
        for(auto s : held_seqs)
            if(s == seq)
                return;

        action a = action::pass;
        if(!data_script.empty())
        {
            a = data_script.front();
            data_script.pop_front();
        }
        switch(a)
        {
            case action::pass: send_to_server(dg); break;
            case action::drop: break; // lost: the ARQ retransmits
            case action::duplicate:
                send_to_server(dg);
                send_to_server(dg);
                break;
            case action::hold:
                held.emplace_back(dg.begin(), dg.end());
                held_seqs.push_back(seq);
                break;
        }
    }

    // Release every held segment (out of order, after later ones already passed) to
    // exercise the receiver's reorder/HOL path end-to-end. Subsequent retransmits of the
    // released seqs then pass normally (the hold no longer applies).
    void release_held()
    {
        for(auto &h : held)
            send_to_server(h);
        held.clear();
        held_seqs.clear();
    }

    void recv_back()
    {
        back.async_receive_from(
                ::asio::buffer(back_buf), from,
                [this](std::error_code ec, std::size_t n)
                {
                    if(ec)
                        return;
                    if(client_ep.port() != 0) // server->client (acks, hs_response) always pass
                        front.send_to(::asio::buffer(back_buf.data(), n), client_ep);
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

// A dialed/accepted channel pair behind a programmable relay, handshake already
// established. The accepted channel records in-order reliable deliveries.
struct fixture
{
    ::asio::io_context                  io;
    pasio::udp_transport                server;
    pasio::udp_transport                client;
    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;
    std::unique_ptr<relay>              link;
    std::vector<std::string>            delivered;
    std::optional<plexus::io::io_error> dialed_error;

    explicit fixture(pio::detail::udp_arq_config cfg = fast_arq())
            : server(io, pasio::udp_channel::default_max_payload,
                     pasio::udp_transport::arq_type::default_ladder, cfg)
            , client(io, pasio::udp_channel::default_max_payload, fast_hs, cfg)
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b)
                                      { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [this] { return server.port() != 0; });

        link = std::make_unique<relay>(io, server.port());

        client.on_dialed(
                [this](std::unique_ptr<pasio::udp_channel> ch, const plexus::io::endpoint &)
                {
                    dialed = std::move(ch);
                    dialed->on_error([this](plexus::io::io_error e) { dialed_error = e; });
                });
        // Dial "udpr" so BOTH ends are reliable_datagram mode: the accepted channel must
        // be reliable-mode to deliver the kind=1 ARQ segments (a best_effort channel drops
        // them — the mode, not the envelope kind alone, gates the reliable path).
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link->port())});
        pump_until(io, [this] { return dialed && accepted; });
    }
};

}

TEST_CASE("udp reliable_arq: a single lost data segment retransmits and delivers in order",
          "[udp][reliable_arq]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f;
        REQUIRE(f.dialed != nullptr);
        REQUIRE(f.accepted != nullptr);

        // Drop the 2nd data segment once: the ARQ must retransmit it under the RTO; the
        // receiver's HOL holds the 3rd/4th behind the gap until the retransmit fills it.
        f.link->data_script = {action::pass, action::drop, action::pass, action::pass};

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "seg-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            REQUIRE(f.dialed->send_reliable(bytes_of(p)) ==
                    pasio::udp_channel::submit_result::admitted);
        }
        pump_until(f.io, [&] { return f.delivered.size() == 4; });

        REQUIRE(f.delivered == sent);    // exactly once, in publish order
        REQUIRE(f.link->data_seen >= 5); // 4 originals + >=1 retransmit (genuinely lossy)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE(
        "udp reliable_arq: out-of-order arrival behind a gap is held then released in order (HOL)",
        "[udp][reliable_arq]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f;
        REQUIRE(f.dialed != nullptr);
        REQUIRE(f.accepted != nullptr);

        // Hold seg 0, pass 1,2,3: the receiver buffers 1,2,3 behind the gap at 0 and
        // delivers NOTHING until 0 is released — then 0,1,2,3 release in publish order.
        f.link->data_script = {action::hold, action::pass, action::pass, action::pass};

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "hol-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            REQUIRE(f.dialed->send_reliable(bytes_of(p)) ==
                    pasio::udp_channel::submit_result::admitted);
        }
        // Let 1,2,3 arrive and be buffered (HOL: nothing delivers behind the gap at 0).
        pump_until(f.io, [&] { return f.link->data_seen >= 4; });
        pump_until(f.io, [&] { return false; }, ms{40});
        REQUIRE(f.delivered.empty()); // HOL: gap at 0 blocks every successor

        f.link->release_held(); // 0 arrives (out of order)
        pump_until(f.io, [&] { return f.delivered.size() == 4; });
        REQUIRE(f.delivered == sent); // released strictly in publish order
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp reliable_arq: a duplicated segment is delivered exactly once", "[udp][reliable_arq]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f;
        REQUIRE(f.dialed != nullptr);
        REQUIRE(f.accepted != nullptr);

        // Duplicate every data segment at the relay: the receiver must deliver each
        // payload exactly once (the reorder buffer drops the duplicate).
        f.link->data_script = {action::duplicate, action::duplicate, action::duplicate};

        std::vector<std::string> sent;
        for(int i = 0; i < 3; ++i)
        {
            const std::string p = "dup-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            REQUIRE(f.dialed->send_reliable(bytes_of(p)) ==
                    pasio::udp_channel::submit_result::admitted);
        }
        pump_until(f.io, [&] { return f.delivered.size() == 3; });
        pump_until(
                f.io, [&] { return false; }, ms{40}); // give any duplicate time to (wrongly) arrive

        REQUIRE(f.delivered == sent); // exactly once despite duplication
        REQUIRE(f.delivered.size() == 3);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp reliable_arq: exhausting the retransmit cap surfaces a connection-fatal error",
          "[udp][reliable_arq]")
{
    constexpr int k_iterations = 20;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        fixture f{exhaust_arq()};
        REQUIRE(f.dialed != nullptr);

        // Drop one segment forever (more drops than the retransmit cap): the ARQ
        // exhausts its budget and surfaces a connection-fatal error, not a silent hang.
        f.link->data_script = std::deque<action>(64, action::drop);

        REQUIRE(f.dialed->send_reliable(bytes_of("never-" + std::to_string(iter))) ==
                pasio::udp_channel::submit_result::admitted);
        pump_until(f.io, [&] { return f.dialed_error.has_value(); });

        REQUIRE(f.dialed_error.has_value());
        REQUIRE(*f.dialed_error == plexus::io::io_error::timed_out);
        REQUIRE(f.delivered.empty()); // the dropped segment never delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp reliable_arq: the bounded send window admits up to W then reports window_full",
          "[udp][reliable_arq]")
{
    // A pure engine unit (no socket): submit W payloads -> all admitted; the W+1th ->
    // window_full (a non-blocking backpressure signal, NOT an io_context stall).
    ::asio::io_context io;
    using engine = pio::detail::udp_reliable_arq<::asio::io_context &, pasio::asio_timer>;
    engine arq{io, pio::detail::udp_arq_config{.window = 4}};
    int    transmits = 0;
    arq.on_transmit([&](std::uint16_t, std::span<const std::byte>, bool) { ++transmits; });

    auto one = bytes_of("x");
    for(int i = 0; i < 4; ++i)
        REQUIRE(arq.submit(one) == engine::submit_result::admitted);
    REQUIRE(arq.submit(one) == engine::submit_result::window_full);
    REQUIRE(arq.in_flight() == 4);
    REQUIRE(transmits == 4);
    arq.cancel(); // single-owner teardown cancels pending timers
    io.poll();
    SUCCEED();
}

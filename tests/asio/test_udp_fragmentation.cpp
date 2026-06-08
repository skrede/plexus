// Live-loopback large-payload fragmentation legs over real sockets: a payload larger
// than the per-datagram budget is SPLIT into numbered fragments at the channel and
// reassembled into ONE message at the peer. Covers, over the real socket (not a
// virtual-clock oracle):
//   * best_effort reassemble: an oversize-but-fragmentable payload round-trips
//     byte-equal as a single reassembled message.
//   * best_effort lost fragment: a withheld fragment leaves the message incomplete and
//     the per-message reassembly timeout drops the WHOLE message (no partial delivery).
//   * reliable reassemble: a large reliable payload round-trips byte-equal, each fragment
//     riding ABOVE the ARQ as one independently-retransmitted segment, so a forced
//     single-fragment loss is retransmitted and the message completes.
// The exhaustive deterministic interleavings are covered sans-IO elsewhere; these prove
// the live composition. The ctest invocation is re-run across >=3 process runs for
// cross-process reproducibility (a live-networking claim is never made from one run).

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/io/fragmentation.h"

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

// A compressed ARQ config so the fragment-loss retransmit is quick. A generous window +
// retransmit cap so a multi-hundred-fragment reliable message rides through the bounded
// congestion=block queue without exhausting retransmits over loopback.
inline pio::detail::udp_arq_config fast_arq()
{
    return pio::detail::udp_arq_config{
        .window = 256, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{120}, .max_retransmit = 20};
}

// A deterministic, position-dependent payload so a reassembled message is byte-checked
// against a regenerated oracle rather than a memcmp of two live buffers.
std::vector<std::byte> make_payload(std::size_t n, std::uint8_t salt)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + salt * 7u + (i >> 8)) & 0xFFu);
    return out;
}

bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    if(a.size() != b.size())
        return false;
    for(std::size_t i = 0; i < a.size(); ++i)
        if(a[i] != b[i])
            return false;
    return true;
}

enum class action { pass, drop };

// A drop-scripting relay between the client and the server socket: acks + handshake +
// best_effort always pass; a scripted reliable data segment is dropped once and the ARQ
// retransmits past the drop. Mirrors the reliable-datagram loss harness; the buffer is
// sized for a default-budget fragment datagram.
struct relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;
    ::asio::ip::udp::socket back;
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 4096> front_buf{};
    std::array<std::byte, 4096> back_buf{};
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
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{8000})
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

TEST_CASE("udp fragment best_effort: an oversize payload reassembles byte-equal as one message",
          "[udp][fragment]")
{
    // A small per-datagram budget so a modest payload fragments into many datagrams; the
    // total stays well inside the loopback socket buffer so a lossless best_effort burst
    // arrives intact and reassembles into ONE message.
    constexpr std::size_t budget = 256;
    constexpr int k_iterations = 20;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, budget, fast_hs};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // ~6 KiB at a 256-byte budget -> ~26 fragments, one logical message.
        auto payload = make_payload(6 * 1024, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() + wire::udp_envelope_overhead > budget);   // genuinely oversize
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                                          // exactly ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload));                        // byte-equal end-to-end
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp fragment best_effort: a lost fragment drops the WHOLE message on the reassembly timeout",
          "[udp][fragment]")
{
    // Inject the fragments of one message directly at the accepting channel but WITHHOLD
    // one: the message never completes, and the per-message reassembly timeout reclaims the
    // whole partial -> no on_data ever fires (no partial delivery).
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, /*budget=*/256, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    int deliveries = 0;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
        accepted = std::move(ch);
        accepted->on_data([&](std::span<const std::byte>) { ++deliveries; });
    });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(accepted != nullptr);

    // Build 4 best_effort fragments of one msg_id and inject all but index 2.
    auto payload = make_payload(1000, 0x5A);
    const std::uint16_t msg_id = 7;
    std::vector<std::vector<std::byte>> datagrams;
    pio::fragment_sink sink = [&](std::uint16_t idx, std::uint16_t cnt, std::span<const std::byte> slice) {
        std::vector<std::byte> dg;
        wire::wrap_udp_fragment_into(dg, wire::udp_envelope_kind::best_effort,
                                     static_cast<std::uint16_t>(idx), msg_id, idx, cnt, slice);
        datagrams.push_back(std::move(dg));
    };
    const std::uint16_t cnt = pio::split(payload, /*budget=*/256, msg_id, sink);
    REQUIRE(cnt >= 3);

    for(std::uint16_t i = 0; i < cnt; ++i)
        if(i != 2)
            accepted->deliver_inbound(datagrams[i]);     // withhold fragment index 2

    // Drain briefly: the message is incomplete, nothing is delivered yet.
    pump_until(io, [&] { return deliveries > 0; }, ms{300});
    REQUIRE(deliveries == 0);

    // The reassembler's per-message timeout (default 5000 ms) reclaims the partial; pump
    // past it and confirm the whole message was dropped (still no delivery).
    pump_until(io, [&] { return deliveries > 0; }, ms{6000});
    REQUIRE(deliveries == 0);                            // dropped whole — no partial delivery
}

TEST_CASE("udp fragment reliable: a large payload reassembles byte-equal with a lost fragment retransmitted",
          "[udp][fragment][reliable]")
{
    // Each fragment of a large reliable payload rides ABOVE the ARQ as one independently-
    // retransmitted segment. A scripted single-fragment loss is selectively retransmitted
    // and the message completes byte-equal end-to-end. The payload is sized so the fragment
    // count stays inside the ARQ window + the bounded congestion=block queue (no synchronous
    // overrun of the unsized window — the throughput interaction above that bound is a
    // separate flow-control concern handled elsewhere).
    constexpr std::size_t budget = 512;
    constexpr int k_iterations = 10;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, budget, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        relay link{io, server.port()};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed->mode() == pio::detail::udp_channel_mode::reliable_datagram);

        // Drop the 3rd reliable data segment once: the ARQ retransmits it and the receiver
        // HOL-holds the later fragments behind the gap until the retransmit fills it.
        link.data_script = {action::pass, action::pass, action::drop};

        // ~24 KiB at a 512-byte budget -> ~50 fragments, inside the 256-segment window.
        auto payload = make_payload(24 * 1024, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() + wire::udp_envelope_overhead + 1 > budget);   // genuinely oversize
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                                            // ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload));                          // byte-equal over loss
        REQUIRE(link.data_seen >= 4);                                        // genuinely lossy (>=1 retransmit)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

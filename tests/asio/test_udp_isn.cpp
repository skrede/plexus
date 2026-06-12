// The randomized per-session initial sequence number (RFC 6528 lineage): the udpr
// reliable-data path no longer starts both ends at a predictable 0. The dialer picks a
// random per-session ISN, advertises it in the UDP handshake inner frame, the acceptor
// echoes it (symmetric, exactly like the channel-mode echo), and both ends feed the
// resolved ISN into the reorder-buffer / ARQ ctor seam. An off-path attacker can then no
// longer inject an accepted [kind=reliable-data][seq=0..] segment on the PLAINTEXT udpr
// path (under AEAD the forged segment also dies at the tag check, so this is the
// plaintext-path defense).
//
// Two halves:
//   (a) the handshake frame carries the ISN append-only — a legacy frame without the ISN
//       bytes decodes to initial_seq = 0 (the documented back-compat contract), an unknown
//       byte still fails closed;
//   (b) live: a normal udpr session round-trips with a non-zero negotiated ISN (no
//       regression), and a spoofed seq=0 reliable-data segment delivered from the peer's
//       source on a non-zero-ISN plaintext udpr session is REJECTED (not delivered).
//
// Each live scenario loops in-body and the ctest invocation is re-run across >=3 process
// runs (a transport claim is never made from a single run). No test-only setter — the ISN
// rides the structural ctor seam (WR-removed the set_initial mutators).

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/io/detail/udp_handshake_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
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

inline pio::detail::udp_arq_config fast_arq()
{
    return pio::detail::udp_arq_config{
        .window = 64, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{80}, .max_retransmit = 12};
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

}

TEST_CASE("udp isn: the handshake frame round-trips the ISN append-only", "[udp][isn]")
{
    namespace iod = pio::detail;

    std::vector<std::byte> out;
    iod::encode_handshake_into(out, iod::udp_hs_type::request, iod::udp_channel_mode::reliable_datagram, 0xBEEF);
    auto decoded = iod::decode_handshake(out);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->type == iod::udp_hs_type::request);
    REQUIRE(decoded->mode == iod::udp_channel_mode::reliable_datagram);
    REQUIRE(decoded->initial_seq == 0xBEEF);
}

TEST_CASE("udp isn: a legacy handshake without the ISN bytes decodes to ISN 0", "[udp][isn]")
{
    namespace iod = pio::detail;

    // A 2-byte [hs_type, channel_mode] frame (the pre-ISN layout) — the ISN bytes absent.
    const std::byte inner[2]{static_cast<std::byte>(iod::udp_hs_type::response),
                             static_cast<std::byte>(iod::udp_channel_mode::reliable_datagram)};
    std::vector<std::byte> legacy;
    wire::wrap_udp_into(legacy, wire::udp_envelope_kind::reliable_arq, 0,
                        std::span<const std::byte>{inner, 2});

    auto decoded = iod::decode_handshake(legacy);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->mode == iod::udp_channel_mode::reliable_datagram);
    REQUIRE(decoded->initial_seq == 0);   // the documented back-compat default
}

TEST_CASE("udp isn: an unknown channel-mode byte still fails closed", "[udp][isn]")
{
    namespace iod = pio::detail;

    const std::byte inner[4]{static_cast<std::byte>(iod::udp_hs_type::request),
                             std::byte{0x7F},   // an undefined mode
                             std::byte{0x11}, std::byte{0x22}};
    std::vector<std::byte> bad;
    wire::wrap_udp_into(bad, wire::udp_envelope_kind::reliable_arq, 0,
                        std::span<const std::byte>{inner, 4});

    REQUIRE(!iod::decode_handshake(bad).has_value());
}

TEST_CASE("udp isn: a normal udpr session round-trips with a non-zero negotiated ISN", "[udp][isn]")
{
    constexpr int k_iterations = 50;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string> delivered;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "isn-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p));
        }
        pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent);          // in-order, exactly once, with a non-zero ISN
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp isn: a spoofed seq=0 reliable-data segment is rejected on a non-zero-ISN plaintext udpr session",
          "[udp][isn][spoof]")
{
    constexpr int k_iterations = 50;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string> delivered;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(accepted != nullptr);

        // An off-path attacker forges a [kind=reliable-data][seq=0] data segment that
        // reaches the receiver as if from the trusted peer (the source endpoint is NOT
        // trusted as identity — a spoofed-source datagram demuxes to this established
        // channel). deliver_inbound is exactly the post-demux entry. With the negotiated ISN
        // non-zero, seq=0 sits OUTSIDE the receiver's in-order window, so the forged payload
        // is never delivered.
        std::vector<std::byte> inner;
        wire::encode_udp_segment_into(inner, bytes_of("FORGED"));
        std::vector<std::byte> spoof;
        wire::wrap_udp_into(spoof, wire::udp_envelope_kind::reliable_arq, 0, inner);
        accepted->deliver_inbound(spoof);

        // Drain any posted work so a (mis)delivery would have surfaced, then assert none did.
        for(int i = 0; i < 64; ++i)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
        REQUIRE(delivered.empty());     // the forged seq=0 was rejected (out of window)
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp isn: forged fragmented-flagged segments the reorder buffer rejects leave no residue",
          "[udp][isn][spoof][frag]")
{
    // A forged reliable-data segment carrying the FRAGMENTED envelope bit at an
    // out-of-window seq must not deposit any per-seq receive-side state: the fragmented
    // flag rides reorder-buffer ACCEPTANCE, so a rejected segment leaves nothing behind and
    // cannot corrupt a subsequent legitimate delivery on the same channel. The observable
    // proof is end-to-end: after a burst of forged fragmented segments, a normal udpr
    // exchange still round-trips byte-identically and nothing forged is ever delivered.
    constexpr int k_iterations = 20;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string> delivered;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // A burst of forged FRAGMENTED-flagged data segments at scattered seqs that sit
        // outside the receiver's in-order window (the negotiated ISN is non-zero, so low
        // seqs are below expected). Pre-fix these inserted into a per-seq set BEFORE the
        // reorder buffer ever validated the seq; post-fix the bit rides acceptance only.
        for(std::uint16_t s = 0; s < 200; ++s)
        {
            std::vector<std::byte> inner;
            wire::encode_udp_segment_into(inner, bytes_of("FORGED-FRAG"));
            std::vector<std::byte> spoof;
            wire::wrap_udp_into_fragmented(spoof, wire::udp_envelope_kind::reliable_arq, s, inner);
            accepted->deliver_inbound(spoof);
        }

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "frag-isn-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p));
        }
        pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent);   // legitimate traffic unharmed; nothing forged delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

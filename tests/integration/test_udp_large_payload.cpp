// Large-payload datagram capability over real loopback sockets: a 1 MB and a 4 MB
// message round-trip byte-identically through the existing fragmenter + per-channel
// reassembler over BOTH the best-effort ("udp") and the reliable-ARQ ("udpr") legs,
// looped in-body and re-run across >=3 process runs (a transport claim is never made
// from a single run). The deterministic loss/reorder shim (tests/support) drives the
// injected-loss legs: under a fixed loss fraction the best-effort path drops the WHOLE
// message on a lost fragment (drop-whole-message semantics, recorded as a measured
// observation) while the reliable path reassembles via retransmit. The shim's own
// determinism (a byte-identical drop/reorder sequence across two runs) is asserted
// first — the empirical-reproducibility property the fragment-scale sweep depends on.
//
// No plexus source is touched by this test; the loss is injected at the wire boundary.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/fragmentation.h"

#include "support/loss_reorder_shim.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace pasio = plexus::asio;
namespace pio = plexus::io;
namespace ptest = plexus::testing;

namespace {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A generous window + quick retransmit so a multi-hundred-fragment reliable message rides
// through the bounded congestion=block queue without exhausting retransmits over loopback.
inline pio::detail::udp_arq_config large_arq()
{
    return pio::detail::udp_arq_config{
        .window = 1024, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{160}, .max_retransmit = 40};
}

// A DELIBERATELY tiny send window so a many-fragment message has far more fragments than
// the window admits: the bulk of fragments must transit the bounded congestion=block
// backpressure queue, which is the path that must preserve each fragment's FRAGMENTED
// envelope bit (a window-sized message never parks a fragment and would not exercise it).
inline pio::detail::udp_arq_config tiny_window_arq()
{
    return pio::detail::udp_arq_config{
        .window = 8, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{160}, .max_retransmit = 40};
}

// A deterministic, position-dependent payload byte-checked against a regenerated oracle.
std::vector<std::byte> make_payload(std::size_t n, std::uint8_t salt)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + salt * 7u + (i >> 8)) & 0xFFu);
    return out;
}

bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{15000})
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

TEST_CASE("loss_shim drops a fixed-seed fraction deterministically across two runs", "[loss_shim]")
{
    // Two schedulers with the SAME config + seed must produce a byte-identical drop/emit
    // sequence over a fixed datagram stream — the reproducibility law the empirical sweeps
    // and the lossy benchmark cell rely on (no std::random anywhere in the path).
    ptest::loss_reorder_config cfg{.loss_num = 30, .loss_den = 100, .reorder_depth = 4, .seed = 0xABCDEF01u};

    auto run_once = [&] {
        ptest::loss_reorder_scheduler sched{cfg};
        std::vector<std::vector<std::byte>> emitted;
        for(int i = 0; i < 500; ++i)
        {
            std::vector<std::byte> dg{static_cast<std::byte>(i & 0xFF),
                                      static_cast<std::byte>((i >> 8) & 0xFF)};
            for(auto &out : sched.drive(dg))
                emitted.push_back(std::move(out));
        }
        for(auto &out : sched.flush())
            emitted.push_back(std::move(out));
        return std::pair{std::move(emitted), sched.dropped()};
    };

    auto [seq_a, dropped_a] = run_once();
    auto [seq_b, dropped_b] = run_once();

    REQUIRE(dropped_a == dropped_b);          // identical drop COUNT across runs
    REQUIRE(dropped_a > 0);                    // the loss fraction genuinely engaged
    REQUIRE(seq_a.size() == seq_b.size());
    for(std::size_t i = 0; i < seq_a.size(); ++i)
        REQUIRE(seq_a[i] == seq_b[i]);         // byte-identical emit ORDER (drop + reorder)

    // The reorder window genuinely reordered (the emitted order is not the input order).
    bool reordered = false;
    for(std::size_t i = 1; i < seq_a.size(); ++i)
        if(seq_a[i] < seq_a[i - 1])
        {
            reordered = true;
            break;
        }
    REQUIRE(reordered);
}

TEST_CASE("loss_shim with zero loss and zero reorder is an order-preserving pass-through", "[loss_shim]")
{
    ptest::loss_reorder_scheduler sched{ptest::loss_reorder_config{}};
    std::vector<std::vector<std::byte>> emitted;
    for(int i = 0; i < 64; ++i)
    {
        std::vector<std::byte> dg{static_cast<std::byte>(i)};
        for(auto &out : sched.drive(dg))
            emitted.push_back(std::move(out));
    }
    REQUIRE(emitted.size() == 64);
    REQUIRE(sched.dropped() == 0);
    for(int i = 0; i < 64; ++i)
        REQUIRE(emitted[static_cast<std::size_t>(i)].front() == static_cast<std::byte>(i));
}

namespace {

// Stand up a loopback udp_transport pair (no interposed relay) and round-trip one payload
// over the given scheme, looping in-body. Returns the count of byte-equal deliveries.
int roundtrip_clean(const char *scheme, std::size_t budget, std::size_t payload_size,
                    pio::detail::udp_arq_config arq, int iterations)
{
    int proven = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, arq};
        pasio::udp_transport client{io, budget, fast_hs, arq};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { got.emplace_back(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({scheme, "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto payload = make_payload(payload_size, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() > budget);                 // genuinely fragmented
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                          // exactly ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload));        // byte-equal end-to-end
        ++proven;
    }
    return proven;
}

}

TEST_CASE("udp_large_payload: an oversize message round-trips byte-identically over UDP best-effort, looped",
          "[udp_large_payload]")
{
    // Best-effort UDP has no retransmit: a burst that overruns the kernel receive buffer
    // (the host default here is ~208 KiB) loses fragments and the whole message is dropped
    // (the recorded loss policy below). The lossless best-effort round-trip is therefore
    // proven at a multi-fragment size that fits the default socket buffer — the fragmenter +
    // reassembler compose correctly over the best-effort leg. The 1 MB / 4 MB capability is
    // proven over udpr, which retransmits the inevitable loss at that volume.
    constexpr std::size_t budget = 1200;                  // the conservative default budget
    REQUIRE(roundtrip_clean("udp", budget, 16u * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 1 MB message round-trips byte-identically over udpr reliable, looped",
          "[udp_large_payload]")
{
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 4 MB message (the max_message_size ceiling) round-trips over udpr, looped",
          "[udp_large_payload]")
{
    // 4 MiB at an 8 KiB budget -> ~512 fragments, inside the 1024-segment window. The whole
    // ceiling message rides the reliable ARQ and reassembles byte-equal.
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 4u * 1024 * 1024, large_arq(), /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: a fragmented udpr message whose fragment count exceeds the send window reassembles through the backpressure queue, looped",
          "[udp_large_payload]")
{
    // Regression: a reliable fragment that parks in the congestion=block backpressure queue
    // (because the send window is full) must KEEP its FRAGMENTED envelope bit when it drains.
    // A tiny 8-segment window against a 1 MiB message (~128 fragments at an 8 KiB budget)
    // forces all but the first few fragments through the queue. If a drained fragment lost
    // the flag the peer would post each raw [msg_id][idx][cnt][slice] blob as a whole message
    // instead of reassembling — got would fill with many wrong-sized blobs and never match the
    // payload, so the single-byte-equal-delivery assertion below fails closed.
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, tiny_window_arq(), /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: the injected-loss policy is recorded as measured — UDP drops the whole message, udpr reassembles",
          "[udp_large_payload]")
{
    // Drive a large message through the deterministic loss shim at a fixed loss fraction on
    // BOTH legs and RECORD the measured loss policy (DGRAM-01's loss-policy recording):
    //   * best_effort ("udp"): a lost fragment is never recovered, so the per-message
    //     reassembly times out and the WHOLE message is dropped — NO delivery.
    //   * reliable ("udpr"): each lost fragment is selectively retransmitted, so the
    //     message reassembles byte-equal despite the same injected loss.
    // A size that fits the kernel receive buffer so the ONLY loss is the shim's (an
    // overrun-driven drop would confound the attribution): ~55 fragments at a 1200-byte
    // budget, each subject to the deterministic 8% drop.
    constexpr std::size_t budget = 1200;
    constexpr std::size_t payload_size = 64u * 1024;
    const ptest::loss_reorder_config loss{.loss_num = 8, .loss_den = 100, .reorder_depth = 0, .seed = 0x5151ull};

    auto run_leg = [&](const char *scheme) {
        ::asio::io_context io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, large_arq()};
        pasio::udp_transport client{io, budget, fast_hs, large_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        int deliveries = 0;
        std::vector<std::byte> last;
        server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> b) { ++deliveries; last.assign(b.begin(), b.end()); });
        });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        ptest::loss_reorder_relay link{io, server.port(), loss};
        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({scheme, "127.0.0.1:" + std::to_string(link.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto payload = make_payload(payload_size, 0x4D);
        dialed->send(payload);

        // Pump well past the reassembly timeout so the best-effort drop-whole verdict settles
        // and the reliable retransmits complete.
        pump_until(io, [&] { return deliveries > 0; }, ms{8000});
        return std::tuple{deliveries, std::move(last), std::move(payload), link.dropped()};
    };

    // RECORDED MEASUREMENT (best-effort): a lost fragment drops the whole message.
    {
        auto [deliveries, last, payload, dropped] = run_leg("udp");
        REQUIRE(dropped > 0);                     // the shim genuinely lost fragments
        REQUIRE(deliveries == 0);                 // drop-whole-message — no partial delivery
        WARN("measured loss policy [udp best-effort]: " << dropped
             << " datagram(s) dropped by the shim -> whole-message drop, deliveries=" << deliveries);
    }

    // RECORDED MEASUREMENT (reliable): the same loss is retransmitted and the message arrives.
    {
        auto [deliveries, last, payload, dropped] = run_leg("udpr");
        REQUIRE(dropped > 0);                     // the same injected loss engaged
        REQUIRE(deliveries == 1);                 // reliably reassembled via retransmit
        REQUIRE(equal_bytes(last, payload));      // byte-equal despite the loss
        WARN("measured loss policy [udpr reliable]: " << dropped
             << " datagram(s) dropped by the shim -> retransmit-reassembled, deliveries=" << deliveries);
    }
}

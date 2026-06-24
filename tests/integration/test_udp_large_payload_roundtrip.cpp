#include "test_udp_large_payload_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace udp_large_payload_fixture;

namespace {

// Stand up a loopback udp_transport pair (no interposed relay) and round-trip one payload
// over the given scheme, looping in-body. Returns the count of byte-equal deliveries.
int roundtrip_clean(const char *scheme, std::size_t budget, std::size_t payload_size, plexus::datagram::detail::udp_arq_config arq, int iterations)
{
    int proven = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, arq};
        pasio::udp_transport client{io, budget, fast_hs, arq};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
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
        REQUIRE(payload.size() > budget); // genuinely fragmented
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                   // exactly ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload)); // byte-equal end-to-end
        ++proven;
    }
    return proven;
}

}

TEST_CASE("udp_large_payload: an oversize message round-trips byte-identically over UDP "
          "best-effort, looped",
          "[udp_large_payload]")
{
    // Best-effort UDP has no retransmit: a burst that overruns the kernel receive buffer
    // (the host default here is ~208 KiB) loses fragments and the whole message is dropped
    // (the recorded loss policy below). The lossless best-effort round-trip is therefore
    // proven at a multi-fragment size that fits the default socket buffer — the fragmenter +
    // reassembler compose correctly over the best-effort leg. The 1 MB / 4 MB capability is
    // proven over udpr, which retransmits the inevitable loss at that volume.
    constexpr std::size_t budget = 1200; // the conservative default budget
    REQUIRE(roundtrip_clean("udp", budget, 16u * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 1 MB message round-trips byte-identically over udpr reliable, looped", "[udp_large_payload]")
{
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, large_arq(), /*iterations=*/8) == 8);
}

TEST_CASE("udp_large_payload: a 4 MB message (the max_message_size ceiling) round-trips over udpr, "
          "looped",
          "[udp_large_payload]")
{
    // 4 MiB at an 8 KiB budget -> ~512 fragments, inside the 1024-segment window. The whole
    // ceiling message rides the reliable ARQ and reassembles byte-equal.
    constexpr std::size_t budget = 8192;
    REQUIRE(roundtrip_clean("udpr", budget, 4u * 1024 * 1024, large_arq(), /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: a fragmented udpr message whose fragment count exceeds the send "
          "window reassembles through the backpressure queue, looped",
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
    REQUIRE(roundtrip_clean("udpr", budget, 1u * 1024 * 1024, tiny_window_arq(),
                            /*iterations=*/4) == 4);
}

TEST_CASE("udp_large_payload: the injected-loss policy is recorded as measured — UDP drops the "
          "whole message, udpr reassembles",
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
    constexpr std::size_t            budget       = 1200;
    constexpr std::size_t            payload_size = 64u * 1024;
    const ptest::loss_reorder_config loss{.loss_num = 8, .loss_den = 100, .reorder_depth = 0, .seed = 0x5151ull};

    auto run_leg = [&](const char *scheme)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, budget, pasio::udp_transport::arq_type::default_ladder, large_arq()};
        pasio::udp_transport client{io, budget, fast_hs, large_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        int                                 deliveries = 0;
        std::vector<std::byte>              last;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data(
                            [&](std::span<const std::byte> b)
                            {
                                ++deliveries;
                                last.assign(b.begin(), b.end());
                            });
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
        REQUIRE(dropped > 0);     // the shim genuinely lost fragments
        REQUIRE(deliveries == 0); // drop-whole-message — no partial delivery
        WARN("measured loss policy [udp best-effort]: " << dropped << " datagram(s) dropped by the shim -> whole-message drop, deliveries=" << deliveries);
    }

    // RECORDED MEASUREMENT (reliable): the same loss is retransmitted and the message arrives.
    {
        auto [deliveries, last, payload, dropped] = run_leg("udpr");
        REQUIRE(dropped > 0);                // the same injected loss engaged
        REQUIRE(deliveries == 1);            // reliably reassembled via retransmit
        REQUIRE(equal_bytes(last, payload)); // byte-equal despite the loss
        WARN("measured loss policy [udpr reliable]: " << dropped << " datagram(s) dropped by the shim -> retransmit-reassembled, deliveries=" << deliveries);
    }
}

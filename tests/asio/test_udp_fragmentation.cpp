#include "test_udp_fragmentation_common.h"

using namespace udp_fragmentation_fixture;

TEST_CASE("udp fragment best_effort: an oversize payload reassembles byte-equal as one message", "[udp][fragment]")
{
    // A small per-datagram budget so a modest payload fragments into many datagrams; the
    // total stays well inside the loopback socket buffer so a lossless best_effort burst
    // arrives intact and reassembles into ONE message.
    constexpr std::size_t budget = 256;
    constexpr int k_iterations   = 20;
    int proven                   = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io};
        pasio::udp_transport client{io, budget, fast_hs};

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
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // ~6 KiB at a 256-byte budget -> ~26 fragments, one logical message.
        auto payload = make_payload(6 * 1024, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() + wire::udp_envelope_overhead > budget); // genuinely oversize
        dialed->send(payload);

        pump_until(io, [&] { return !got.empty(); });
        REQUIRE(got.size() == 1);                   // exactly ONE reassembled message
        REQUIRE(equal_bytes(got.front(), payload)); // byte-equal end-to-end
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp fragment best_effort: a lost fragment drops the WHOLE message on the reassembly "
          "timeout",
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
    server.on_accepted(
            [&](std::unique_ptr<pasio::udp_channel> ch)
            {
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
    auto payload               = make_payload(1000, 0x5A);
    const std::uint16_t msg_id = 7;
    std::vector<std::vector<std::byte>> datagrams;
    pio::fragment_sink sink = [&](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> slice)
    {
        std::vector<std::byte> dg;
        wire::wrap_udp_fragment_into(dg, wire::udp_envelope_kind::best_effort, static_cast<std::uint16_t>(idx), msg_id, idx, cnt, slice);
        datagrams.push_back(std::move(dg));
    };
    const std::uint32_t cnt = pio::split(payload, /*budget=*/256, msg_id, sink);
    REQUIRE(cnt >= 3);

    for(std::uint32_t i = 0; i < cnt; ++i)
        if(i != 2)
            accepted->deliver_inbound(datagrams[i]); // withhold fragment index 2

    // Drain briefly: the message is incomplete, nothing is delivered yet.
    pump_until(io, [&] { return deliveries > 0; }, ms{300});
    REQUIRE(deliveries == 0);

    // The reassembler's per-message timeout (default 5000 ms) reclaims the partial; pump
    // past it and confirm the whole message was dropped (still no delivery).
    pump_until(io, [&] { return deliveries > 0; }, ms{6000});
    REQUIRE(deliveries == 0); // dropped whole — no partial delivery
}

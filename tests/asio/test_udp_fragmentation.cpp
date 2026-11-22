#include "test_udp_fragmentation_common.h"

#include <algorithm>

using namespace udp_fragmentation_fixture;

TEST_CASE("udp fragment best_effort: an oversize payload reassembles byte-equal as one message", "[udp][fragment]")
{
    // A small per-datagram budget so a modest payload fragments into many datagrams; the
    // total stays well inside the loopback socket buffer so a lossless best_effort burst
    // arrives intact and reassembles into ONE message.
    constexpr std::size_t budget        = 256;
    constexpr std::int32_t k_iterations = 20;
    std::int32_t proven                 = 0;
    for(std::int32_t iter = 0; iter < k_iterations; ++iter)
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
        plexus::testing::pump_until(io, [&] { return server.port() != 0; }, k_pump_bound);

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        plexus::testing::pump_until(io, [&] { return dialed && accepted; }, k_pump_bound);
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        auto payload = make_payload(6 * 1024, static_cast<std::uint8_t>(iter));
        REQUIRE(payload.size() + wire::udp_envelope_overhead > budget);
        dialed->send(payload);

        plexus::testing::pump_until(io, [&] { return !got.empty(); }, k_pump_bound);
        REQUIRE(got.size() == 1);
        REQUIRE(equal_bytes(got.front(), payload));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("udp fragment best_effort: a lost fragment drops the WHOLE message on the reassembly "
          "timeout",
          "[udp][fragment]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, /*budget=*/256, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::int32_t deliveries = 0;
    server.on_accepted(
            [&](std::unique_ptr<pasio::udp_channel> ch)
            {
                accepted = std::move(ch);
                accepted->on_data([&](std::span<const std::byte>) { ++deliveries; });
            });
    server.listen({"udp", "127.0.0.1:0"});
    plexus::testing::pump_until(io, [&] { return server.port() != 0; }, k_pump_bound);

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return dialed && accepted; }, k_pump_bound);
    REQUIRE(accepted != nullptr);

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
            accepted->deliver_inbound(datagrams[i]);

    plexus::testing::pump_until(io, [&] { return deliveries > 0; }, ms{300});
    REQUIRE(deliveries == 0);

    // The reassembler's per-message timeout (default 5000 ms) reclaims the partial.
    plexus::testing::pump_until(io, [&] { return deliveries > 0; }, ms{6000});
    REQUIRE(deliveries == 0);
}

TEST_CASE("udp fragment reliable: every fragment datagram of a large reliable send fits the transport budget", "[udp][fragment][reliable]")
{
    constexpr std::size_t budget = 512;
    ::asio::io_context io;
    const auto loopback_ep = [](std::uint16_t port) { return ::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), port); };
    pasio::udp_server client{io};
    pasio::udp_server sniffer{io};
    client.start(loopback_ep(0));
    sniffer.start(loopback_ep(0));

    std::size_t largest = 0;
    std::uint32_t seen  = 0;
    sniffer.on_datagram(
            [&](const ::asio::ip::udp::endpoint &, std::span<const std::byte> datagram)
            {
                largest = std::max(largest, datagram.size());
                ++seen;
            });

    pasio::udp_channel ch{io,
                          client,
                          loopback_ep(sniffer.port()),
                          budget,
                          fast_arq(),
                          pio::congestion::block,
                          pasio::udp_channel::default_backpressure_bytes,
                          plexus::datagram::detail::udp_channel_mode::reliable_datagram,
                          0};
    const auto payload = make_payload(4096, 0x3C);
    ch.send(payload);

    plexus::testing::pump_until(io, [&] { return seen >= pio::fragment_count(payload.size(), budget); }, k_pump_bound);
    REQUIRE(seen >= pio::fragment_count(payload.size(), budget));
    REQUIRE(largest <= budget);
}

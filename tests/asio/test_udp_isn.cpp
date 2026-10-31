#include "test_udp_isn_common.h"

using namespace udp_isn_fixture;

TEST_CASE("udp isn: the handshake frame round-trips its type, mode and ISN", "[udp][isn]")
{
    namespace iod = plexus::datagram::detail;

    std::vector<std::byte> out;
    iod::encode_handshake_into(out, iod::udp_hs_type::request, iod::udp_channel_mode::reliable_datagram, 0xBEEF);
    auto decoded = iod::decode_handshake(out);
    REQUIRE(decoded.has_value());
    REQUIRE(decoded->type == iod::udp_hs_type::request);
    REQUIRE(decoded->mode == iod::udp_channel_mode::reliable_datagram);
    REQUIRE(decoded->initial_seq == 0xBEEF);
}

TEST_CASE("udp isn: an inner handshake frame of any length but four bytes is refused", "[udp][isn]")
{
    namespace iod = plexus::datagram::detail;

    const std::byte inner[8]{static_cast<std::byte>(iod::udp_hs_type::request), static_cast<std::byte>(iod::udp_channel_mode::reliable_datagram), std::byte{0x11}, std::byte{0x22}};
    for(const std::size_t length : std::initializer_list<std::size_t>{0, 1, 2, 3, 5, 6, 8})
    {
        CAPTURE(length);
        std::vector<std::byte> datagram;
        wire::wrap_udp_into(datagram, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, length});
        CHECK(!iod::decode_handshake(datagram).has_value());
    }
}

TEST_CASE("udp isn: a handshake under a nonzero envelope sequence is refused", "[udp][isn]")
{
    namespace iod = plexus::datagram::detail;

    const std::byte inner[4]{static_cast<std::byte>(iod::udp_hs_type::request), static_cast<std::byte>(iod::udp_channel_mode::reliable_datagram), std::byte{0x11}, std::byte{0x22}};
    for(const std::uint16_t seq : std::initializer_list<std::uint16_t>{1, 0x00FF, 0x8000, 0xFFFF})
    {
        CAPTURE(seq);
        std::vector<std::byte> datagram;
        wire::wrap_udp_into(datagram, wire::udp_envelope_kind::reliable_arq, seq, std::span<const std::byte>{inner, 4});
        CHECK(!iod::decode_handshake(datagram).has_value());
    }
}

TEST_CASE("udp isn: a handshake with any envelope flag bit set is refused", "[udp][isn]")
{
    namespace iod = plexus::datagram::detail;

    const std::byte inner[4]{static_cast<std::byte>(iod::udp_hs_type::response), static_cast<std::byte>(iod::udp_channel_mode::best_effort), std::byte{0x11}, std::byte{0x22}};
    std::vector<std::byte> fragmented;
    wire::wrap_udp_into_fragmented(fragmented, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 4});
    CHECK(!iod::decode_handshake(fragmented).has_value());
    for(std::uint8_t bit = 1; bit < wire::detail::udp_kind_shift; ++bit)
    {
        CAPTURE(bit);
        std::vector<std::byte> flagged;
        wire::wrap_udp_into(flagged, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 4});
        flagged[0] |= std::byte{static_cast<std::uint8_t>(1u << bit)};
        CHECK(!iod::decode_handshake(flagged).has_value());
    }
}

TEST_CASE("udp isn: an unknown channel-mode byte still fails closed", "[udp][isn]")
{
    namespace iod = plexus::datagram::detail;

    const std::byte inner[4]{static_cast<std::byte>(iod::udp_hs_type::request), std::byte{0x7F}, // an undefined mode
                             std::byte{0x11}, std::byte{0x22}};
    std::vector<std::byte> bad;
    wire::wrap_udp_into(bad, wire::udp_envelope_kind::reliable_arq, 0, std::span<const std::byte>{inner, 4});

    REQUIRE(!iod::decode_handshake(bad).has_value());
}

TEST_CASE("udp isn: a normal udpr session round-trips with a non-zero negotiated ISN", "[udp][isn]")
{
    constexpr int k_iterations = 50;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload, pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string> delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        plexus::testing::pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        plexus::testing::pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "isn-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p));
        }
        plexus::testing::pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent); // in-order, exactly once, with a non-zero ISN
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

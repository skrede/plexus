#include "test_udp_isn_common.h"

using namespace udp_isn_fixture;

TEST_CASE("udp isn: the handshake frame round-trips the ISN append-only", "[udp][isn]")
{
    namespace iod = pio::detail;

    std::vector<std::byte> out;
    iod::encode_handshake_into(out, iod::udp_hs_type::request,
                               iod::udp_channel_mode::reliable_datagram, 0xBEEF);
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
    REQUIRE(decoded->initial_seq == 0); // the documented back-compat default
}

TEST_CASE("udp isn: an unknown channel-mode byte still fails closed", "[udp][isn]")
{
    namespace iod = pio::detail;

    const std::byte        inner[4]{static_cast<std::byte>(iod::udp_hs_type::request),
                                    std::byte{0x7F}, // an undefined mode
                                    std::byte{0x11}, std::byte{0x22}};
    std::vector<std::byte> bad;
    wire::wrap_udp_into(bad, wire::udp_envelope_kind::reliable_arq, 0,
                        std::span<const std::byte>{inner, 4});

    REQUIRE(!iod::decode_handshake(bad).has_value());
}

TEST_CASE("udp isn: a normal udpr session round-trips with a non-zero negotiated ISN", "[udp][isn]")
{
    constexpr int k_iterations = 50;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context   io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload,
                                    pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs,
                                    fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string>            delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
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

        REQUIRE(delivered == sent); // in-order, exactly once, with a non-zero ISN
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#include "test_udp_reliable_datagram_common.h"

#include <stdexcept>

using namespace udp_reliable_datagram_fixture;

TEST_CASE("udp reliable_datagram: a 'udpr' dial mints reliable-mode channels on BOTH ends "
          "(symmetric)",
          "[udp][reliable_datagram][mode]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    plexus::testing::pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return dialed && accepted; });

    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);
    REQUIRE(dialed->mode() == plexus::datagram::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(accepted->mode() == plexus::datagram::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(dialed->remote_endpoint().scheme == "udpr");
    REQUIRE(accepted->remote_endpoint().scheme == "udpr");
}

TEST_CASE("udp reliable_datagram: a 'udp' dial stays best_effort (the opt-in is scheme-gated)", "[udp][reliable_datagram][mode]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    plexus::testing::pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return dialed && accepted; });

    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);
    REQUIRE(dialed->mode() == plexus::datagram::detail::udp_channel_mode::best_effort);
    REQUIRE(accepted->mode() == plexus::datagram::detail::udp_channel_mode::best_effort);
    REQUIRE(dialed->remote_endpoint().scheme == "udp");
    REQUIRE(accepted->remote_endpoint().scheme == "udp");
}

TEST_CASE("udp reliable_datagram: a message beyond the max-message size is rejected at publish "
          "(cross-class)",
          "[udp][reliable_datagram][oversize]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::optional<pio::io_error> dialed_error;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    plexus::testing::pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed(
            [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
            {
                dialed = std::move(ch);
                dialed->on_error([&](pio::io_error e) { dialed_error = e; });
            });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    plexus::testing::pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(dialed != nullptr);

    std::vector<std::byte> too_big(pio::global_default_max_message_bytes + 1, std::byte{0x5A});
    dialed->send(too_big);
    plexus::testing::pump_until(io, [&] { return dialed_error.has_value(); });

    REQUIRE(dialed_error.has_value());
    REQUIRE(*dialed_error == pio::io_error::message_too_large);
    REQUIRE(dialed->is_open());
}

TEST_CASE("udp reliable_datagram: a transport or channel configured to drop its oldest frames is refused when built", "[udp][reliable_datagram]")
{
    ::asio::io_context io;
    CHECK_THROWS_AS(pasio::udp_transport(io, pasio::udp_channel::default_max_payload, fast_hs, {}, pio::congestion::drop_oldest), std::runtime_error);

    pasio::udp_server server{io};
    const ::asio::ip::udp::endpoint dest{::asio::ip::address_v4::loopback(), 9};
    CHECK_THROWS_AS(pasio::udp_channel(io, server, dest, pasio::udp_channel::default_max_payload, {}, pio::congestion::drop_oldest), std::runtime_error);
}

TEST_CASE("udp reliable_datagram: a transport or channel whose fragment budget cannot carry the smallest fragment is refused when built", "[udp][reliable_datagram]")
{
    constexpr std::size_t smallest_budget = 142;
    ::asio::io_context io;
    CHECK_THROWS_AS(pasio::udp_transport(io, smallest_budget - 1, fast_hs), std::runtime_error);
    CHECK_NOTHROW(pasio::udp_transport(io, smallest_budget, fast_hs));

    pasio::udp_server server{io};
    const ::asio::ip::udp::endpoint dest{::asio::ip::address_v4::loopback(), 9};
    CHECK_THROWS_AS(pasio::udp_channel(io, server, dest, smallest_budget - 1), std::runtime_error);
    CHECK_NOTHROW(pasio::udp_channel(io, server, dest, smallest_budget));
}

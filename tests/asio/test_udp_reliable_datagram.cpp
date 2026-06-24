#include "test_udp_reliable_datagram_common.h"

using namespace udp_reliable_datagram_fixture;

TEST_CASE("udp reliable_datagram: a 'udpr' dial mints reliable-mode channels on BOTH ends "
          "(symmetric)",
          "[udp][reliable_datagram][mode]")
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });

    REQUIRE(dialed != nullptr);
    REQUIRE(accepted != nullptr);
    // The dialer declared reliable_datagram in the handshake; the acceptor minted the
    // SAME mode — both report "udpr" (the mode is symmetric, not just dialer-side).
    REQUIRE(dialed->mode() == plexus::datagram::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(accepted->mode() == plexus::datagram::detail::udp_channel_mode::reliable_datagram);
    REQUIRE(dialed->remote_endpoint().scheme == "udpr");
    REQUIRE(accepted->remote_endpoint().scheme == "udpr");
}

TEST_CASE("udp reliable_datagram: a 'udp' dial stays best_effort (the opt-in is scheme-gated)", "[udp][reliable_datagram][mode]")
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });

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
    // The oversize-reject-at-publish invariant holds on BOTH classes for the genuinely-
    // too-big message: a reliable send() of a payload beyond the bounded max-MESSAGE size
    // surfaces message_too_large at publish (the channel stays open), exactly like the
    // best_effort path — never a silent drop. A merely-oversize payload is fragmented.
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::optional<pio::io_error>        dialed_error;
    server.on_accepted([&](std::unique_ptr<pasio::udp_channel> ch) { accepted = std::move(ch); });
    server.listen({"udp", "127.0.0.1:0"});
    pump_until(io, [&] { return server.port() != 0; });

    client.on_dialed(
            [&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
            {
                dialed = std::move(ch);
                dialed->on_error([&](pio::io_error e) { dialed_error = e; });
            });
    client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
    pump_until(io, [&] { return dialed && accepted; });
    REQUIRE(dialed != nullptr);

    // A payload beyond the bounded max-MESSAGE size is rejected at publish via
    // on_error(message_too_large) — the reliable class enforces the same hard ceiling as
    // best_effort. A merely-oversize-but-fragmentable payload is split, not rejected.
    std::vector<std::byte> too_big(pio::global_default_max_message_bytes + 1, std::byte{0x5A});
    dialed->send(too_big); // reliable-mode send dispatches to send_reliable -> oversize reject
    pump_until(io, [&] { return dialed_error.has_value(); });

    REQUIRE(dialed_error.has_value());
    REQUIRE(*dialed_error == pio::io_error::message_too_large);
    REQUIRE(dialed->is_open()); // rejected at publish, channel stays open
}

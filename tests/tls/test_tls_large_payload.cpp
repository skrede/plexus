#include "test_tls_large_payload_common.h"

using namespace tls_large_payload_fixture;

TEST_CASE("tls.large: a 16 MB single frame round-trips byte-identically over a live mutual-TLS "
          "loopback pair, looped",
          "[tls][envelope16]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    constexpr std::size_t k_payload = 16u * 1024u * 1024u;
    constexpr std::size_t k_ceiling = 20u * 1024u * 1024u;
    constexpr std::size_t k_budget  = 48u * 1024u * 1024u;
    constexpr std::size_t k_outbox  = k_ceiling + 4u * 1024u * 1024u;

    const auto         body = ramp_payload(k_payload);
    wire::frame_header hdr{};
    hdr.type         = wire::msg_type::unidirectional;
    hdr.payload_len  = body.size();
    const auto frame = wire::encode_frame(hdr, std::span<const std::byte>{body});

    constexpr int k_iterations = 2;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        auto               server_cred = make_cred(server_id, client_id.digest);
        auto               client_cred = make_cred(client_id, server_id.digest);
        // The node-options surface: raise the receive ceiling + aggregate budget + send outbox
        // for the 16 MB path. The intermediate ctor params keep their defaults.
        ptls::tls_transport server{io,
                                   server_cred,
                                   stream::stream_inbound_config{},
                                   true,
                                   pio::congestion::block,
                                   pio::egress_capacity::of_bytes(k_outbox),
                                   {},
                                   k_ceiling,
                                   k_budget};
        ptls::tls_transport client{io,
                                   client_cred,
                                   stream::stream_inbound_config{},
                                   true,
                                   pio::congestion::block,
                                   pio::egress_capacity::of_bytes(k_outbox),
                                   {},
                                   k_ceiling,
                                   k_budget};

        std::unique_ptr<ptls::tls_channel> accepted, dialed;
        std::vector<std::byte>             got;
        std::optional<wire::close_cause>   closed;
        bool                               dial_failed = false;
        server.on_accepted(
                [&](std::unique_ptr<ptls::tls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d)
                                      { got.assign(d.begin(), d.end()); });
                    accepted->on_protocol_close([&](wire::close_cause c) { closed = c; });
                });
        client.on_dialed([&](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.on_dial_failed([&](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        server.listen({"tls", "127.0.0.1:0"});
        client.dial({"tls", "127.0.0.1:" + std::to_string(server.port())});

        auto handshake_bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!((accepted && dialed) || dial_failed) &&
              std::chrono::steady_clock::now() < handshake_bound)
            io.poll();
        REQUIRE_FALSE(dial_failed);
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while(got.size() < frame.size() && !closed && std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE_FALSE(closed.has_value()); // the receive ceiling admitted the encrypted 16 MB frame
        REQUIRE(got.size() == frame.size());
        REQUIRE(got == frame); // byte-equal reassembly over the secure channel
        const auto delivered_body = std::span<const std::byte>{got}.subspan(wire::header_size);
        REQUIRE(std::equal(delivered_body.begin(), delivered_body.end(), body.begin()));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

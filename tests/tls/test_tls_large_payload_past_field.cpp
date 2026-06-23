#include "test_tls_large_payload_common.h"

using namespace tls_large_payload_fixture;

TEST_CASE("tls.large_past_field: a 32 MB single frame round-trips byte-identically over a live "
          "mutual-TLS loopback pair, exceeding the field's 16 MB socket ceiling, looped",
          "[tls][envelope16]")
{
    // The opt-in raised ceiling pushes a single secure-stream message PAST 16 MB — the socket
    // max-payload ceiling the field caps at. A 32 MB frame round-trips byte-equal over a live
    // mutual-TLS loopback pair with the node receive ceiling + aggregate budget + send outbox
    // raised through the transport ctor (same positions the 16 MB case uses). Big-memory test:
    // iterations kept low. A position ramp in the body catches a reorder/corruption.
    identity_fixture server_id("srv_past");
    identity_fixture client_id("cli_past");

    constexpr std::size_t k_payload = 32u * 1024u * 1024u; // > the field's 16 MB socket ceiling
    constexpr std::size_t k_ceiling = 36u * 1024u * 1024u;
    constexpr std::size_t k_budget  = 80u * 1024u * 1024u;
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
        ::asio::io_context  io;
        auto                server_cred = make_cred(server_id, client_id.digest);
        auto                client_cred = make_cred(client_id, server_id.digest);
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
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while(got.size() < frame.size() && !closed && std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE_FALSE(closed.has_value()); // the raised ceiling admitted the encrypted 32 MB frame
        REQUIRE(got.size() == frame.size());
        REQUIRE(got == frame); // byte-equal reassembly past the field's 16 MB ceiling
        const auto delivered_body = std::span<const std::byte>{got}.subspan(wire::header_size);
        REQUIRE(std::equal(delivered_body.begin(), delivered_body.end(), body.begin()));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#include "test_dtls_handshake_common.h"

namespace {

// A DIALED loopback DTLS link via the transport: a server-side and a client-side
// dtls_transport (each with its own cross-pinning credential) over one io_context.
// listen("dtls","127.0.0.1:0") then dial the bound port; the accepted channel lands
// via on_accepted (post mutual-verify), the dialed channel via on_dialed (post
// external_complete) CARRYING the dialed endpoint.
struct dial_link
{
    ::asio::io_context   io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    pdt::pio::endpoint                  dialed_ep;
    bool                                dial_failed{false};
    std::vector<std::vector<std::byte>> server_received;

    dial_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
            , server(io, server_cred)
            , client(io, client_cred)
    {
        server.on_accepted(
                [this](std::unique_ptr<ptls::dtls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
                });
        client.on_dialed(
                [this](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &ep)
                {
                    dialed    = std::move(ch);
                    dialed_ep = ep;
                });
        client.on_dial_failed([this](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        client.dial({"dtls", "127.0.0.1:" + std::to_string(server.port())});
    }

    template<typename Pred>
    void pump_until(Pred pred, std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        pdt::pump_until(io, pred, timeout);
    }
};

}

TEST_CASE("dtls.handshake: a dialed transport pair completes a mutual DTLS handshake over "
          "loopback, looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("tx_srv");
    pdt::identity_fixture client_id("tx_cli");

    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        dial_link l(server_id, client_id);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });

        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr); // delivered POST external_complete only
        REQUIRE(l.dialed->remote_endpoint().scheme == "dtls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.external_complete: the FSM resolves with cert identity and no plexus wire frame, "
          "looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ec_srv");
    pdt::identity_fixture client_id("ec_cli");

    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        dial_link l(server_id, client_id);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });
        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);

        // Identity is cert-derived (R-OQ3): node_name == cert subject, node_id == SPKI.
        REQUIRE(l.dialed->peer_node_name() == server_id.subject);
        REQUIRE(l.accepted->peer_node_name() == client_id.subject);
        REQUIRE(std::memcmp(l.dialed->peer_node_id().data(), server_id.digest.data(), 16) == 0);
        REQUIRE(std::memcmp(l.accepted->peer_node_id().data(), client_id.digest.data(), 16) == 0);

        // The dialed endpoint is carried through on_dialed (the crypto handshake
        // resolved the session — no plexus handshake_request/response frame was sent).
        REQUIRE(l.dialed_ep.scheme == "dtls");

        // An app frame flows decrypted — proves the resolved session is a live channel.
        const std::string      payload = "post-external-complete-frame";
        std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(payload.data()), reinterpret_cast<const std::byte *>(payload.data()) + payload.size());
        l.dialed->send(std::span<const std::byte>{frame});
        l.pump_until([&] { return !l.server_received.empty(); });
        REQUIRE(l.server_received.size() == 1);
        REQUIRE(l.server_received[0] == frame);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

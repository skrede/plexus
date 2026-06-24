#include "test_dtls_handshake_common.h"

namespace {

// A direct two-channel loopback: a client and a server dtls_channel each over its
// own udp_server bound to 127.0.0.1:0, wired so each datagram a socket receives is
// fed to the OTHER channel's deliver_inbound (a manual one-peer relay). This drives
// the channel BIO-pair pump end-to-end WITHOUT the transport, so the channel-level
// handshake + completion edge is exercised in isolation.
struct channel_link
{
    ::asio::io_context                  io;
    ptls::tls_credential                server_cred;
    ptls::tls_credential                client_cred;
    plexus::io::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    plexus::io::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server                   server_sock{io};
    pasio::udp_server                   client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool                                server_complete{false};
    bool                                client_complete{false};
    std::vector<std::vector<std::byte>> server_received;

    std::vector<std::vector<std::byte>> client_to_server; // datagrams the server socket got
    std::vector<std::vector<std::byte>> server_to_client; // datagrams the client socket got

    channel_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
            : server_cred(pdt::pin_one(server_id, client_id.digest))
            , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred, server_cookie, ptls::dtls_channel::role::server);
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred, client_cookie, ptls::dtls_channel::role::client);

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { client_to_server.emplace_back(b.begin(), b.end()); });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { server_to_client.emplace_back(b.begin(), b.end()); });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
    }

    void pump_relays()
    {
        auto cs = std::move(client_to_server);
        client_to_server.clear();
        for(auto &dg : cs)
            server_ch->deliver_inbound(std::span<const std::byte>{dg});
        auto sc = std::move(server_to_client);
        server_to_client.clear();
        for(auto &dg : sc)
            client_ch->deliver_inbound(std::span<const std::byte>{dg});
    }

    void run(std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        server_ch->start_handshake();
        client_ch->start_handshake();
        auto bound = std::chrono::steady_clock::now() + timeout;
        while((!server_complete || !client_complete) && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
    }
};

}

TEST_CASE("dtls.channel_handshake: two channels complete a mutual handshake over loopback, looped", "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ch_srv");
    pdt::identity_fixture client_id("ch_cli");

    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        channel_link l(server_id, client_id);
        l.run();
        REQUIRE(l.server_complete);
        REQUIRE(l.client_complete);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.channel_identity: completion yields the peer SPKI node_id and cert-subject node_name", "[dtls][handshake]")
{
    pdt::identity_fixture server_id("id_srv");
    pdt::identity_fixture client_id("id_cli");

    channel_link l(server_id, client_id);
    l.run();
    REQUIRE(l.server_complete);
    REQUIRE(l.client_complete);

    REQUIRE(l.client_ch->peer_node_name() == server_id.subject);
    REQUIRE(l.server_ch->peer_node_name() == client_id.subject);

    REQUIRE(std::memcmp(l.client_ch->peer_node_id().data(), server_id.digest.data(), 16) == 0);
    REQUIRE(std::memcmp(l.server_ch->peer_node_id().data(), client_id.digest.data(), 16) == 0);
}

TEST_CASE("dtls.channel_appdata: an app frame flows decrypted post-handshake, looped", "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ad_srv");
    pdt::identity_fixture client_id("ad_cli");

    const std::string      payload = "secret-datagram-over-dtls";
    std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(payload.data()), reinterpret_cast<const std::byte *>(payload.data()) + payload.size());

    constexpr int k_iterations = 100;
    int           delivered    = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        channel_link l(server_id, client_id);
        l.run();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        l.client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + std::chrono::milliseconds{1000};
        while(l.server_received.empty() && std::chrono::steady_clock::now() < bound)
        {
            l.io.poll();
            if(l.io.stopped())
                l.io.restart();
            l.pump_relays();
        }
        REQUIRE(l.server_received.size() == 1);
        REQUIRE(l.server_received[0] == frame);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

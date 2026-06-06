#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/asio/udp_server.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstring>
#include <cstddef>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pasio = plexus::asio;

namespace {

// A direct two-channel loopback: a client and a server dtls_channel each over its
// own udp_server bound to 127.0.0.1:0, wired so each datagram a socket receives is
// fed to the OTHER channel's deliver_inbound (a manual one-peer relay). This drives
// the channel BIO-pair pump end-to-end WITHOUT the transport, so the channel-level
// handshake + completion edge is exercised in isolation.
struct channel_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_cookie_state server_cookie;
    ptls::dtls_cookie_state client_cookie;
    pasio::udp_server server_sock{io};
    pasio::udp_server client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool server_complete{false};
    bool client_complete{false};
    std::vector<std::vector<std::byte>> server_received;

    std::vector<std::vector<std::byte>> client_to_server;   // datagrams the server socket got
    std::vector<std::vector<std::byte>> server_to_client;   // datagrams the client socket got

    channel_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred,
                                                         server_cookie, ptls::dtls_channel::role::server);
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred,
                                                         client_cookie, ptls::dtls_channel::role::client);

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            client_to_server.emplace_back(b.begin(), b.end());
        });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            server_to_client.emplace_back(b.begin(), b.end());
        });

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

TEST_CASE("dtls.channel_handshake: two channels complete a mutual handshake over loopback, looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ch_srv");
    pdt::identity_fixture client_id("ch_cli");

    constexpr int k_iterations = 100;
    int completed = 0;
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

TEST_CASE("dtls.channel_identity: completion yields the peer SPKI node_id and cert-subject node_name",
          "[dtls][handshake]")
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

TEST_CASE("dtls.channel_appdata: an app frame flows decrypted post-handshake, looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ad_srv");
    pdt::identity_fixture client_id("ad_cli");

    const std::string payload = "secret-datagram-over-dtls";
    std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(payload.data()),
                                 reinterpret_cast<const std::byte *>(payload.data()) + payload.size());

    constexpr int k_iterations = 100;
    int delivered = 0;
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

namespace {

// A DIALED loopback DTLS link via the transport: a server-side and a client-side
// dtls_transport (each with its own cross-pinning credential) over one io_context.
// listen("dtls","127.0.0.1:0") then dial the bound port; the accepted channel lands
// via on_accepted (post mutual-verify), the dialed channel via on_dialed (post
// external_complete) CARRYING the dialed endpoint.
struct dial_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    pdt::pio::endpoint dialed_ep;
    bool dial_failed{false};
    std::vector<std::vector<std::byte>> server_received;

    dial_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
        , server(io, server_cred)
        , client(io, client_cred)
    {
        server.on_accepted([this](std::unique_ptr<ptls::dtls_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([this](std::span<const std::byte> d) {
                server_received.emplace_back(d.begin(), d.end());
            });
        });
        client.on_dialed([this](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &ep) {
            dialed = std::move(ch);
            dialed_ep = ep;
        });
        client.on_dial_failed([this](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        client.dial({"dtls", "127.0.0.1:" + std::to_string(server.port())});
    }

    template <typename Pred>
    void pump_until(Pred pred, std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
    {
        pdt::pump_until(io, pred, timeout);
    }
};

}

TEST_CASE("dtls.handshake: a dialed transport pair completes a mutual DTLS handshake over loopback, looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("tx_srv");
    pdt::identity_fixture client_id("tx_cli");

    constexpr int k_iterations = 100;
    int completed = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        dial_link l(server_id, client_id);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });

        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr);                 // delivered POST external_complete only
        REQUIRE(l.dialed->remote_endpoint().scheme == "dtls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.external_complete: the FSM resolves with cert identity and no plexus wire frame, looped",
          "[dtls][handshake]")
{
    pdt::identity_fixture server_id("ec_srv");
    pdt::identity_fixture client_id("ec_cli");

    constexpr int k_iterations = 100;
    int proven = 0;
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

        // The dialed endpoint is carried through on_dialed (D-01: the crypto handshake
        // resolved the session — no plexus handshake_request/response frame was sent).
        REQUIRE(l.dialed_ep.scheme == "dtls");

        // An app frame flows decrypted — proves the resolved session is a live channel.
        const std::string payload = "post-external-complete-frame";
        std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(payload.data()),
                                     reinterpret_cast<const std::byte *>(payload.data()) + payload.size());
        l.dialed->send(std::span<const std::byte>{frame});
        l.pump_until([&] { return !l.server_received.empty(); });
        REQUIRE(l.server_received.size() == 1);
        REQUIRE(l.server_received[0] == frame);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

namespace {

// A dialed link where the client dials a programmable lossy relay that forwards to
// the real server, so the handshake flights can be dropped/reordered/duplicated.
// The cred pair is injected so the verify cases can mis-pin either side.
struct relay_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::dtls_transport server;
    ptls::dtls_transport client;
    std::unique_ptr<pdt::relay> link;

    std::unique_ptr<ptls::dtls_channel> accepted;
    std::unique_ptr<ptls::dtls_channel> dialed;
    bool dial_failed{false};

    relay_link(ptls::tls_credential srv, ptls::tls_credential cli)
        : server_cred(std::move(srv))
        , client_cred(std::move(cli))
        , server(io, server_cred)
        , client(io, client_cred)
    {
        server.on_accepted([this](std::unique_ptr<ptls::dtls_channel> ch) { accepted = std::move(ch); });
        client.on_dialed([this](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &) { dialed = std::move(ch); });
        client.on_dial_failed([this](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        link = std::make_unique<pdt::relay>(io, server.port());
        client.dial({"dtls", "127.0.0.1:" + std::to_string(link->port())});
    }
};

}

TEST_CASE("dtls.verify: a cross-pinned pair accepts; mis-pin / unpinned / empty fail closed, looped",
          "[dtls][verify]")
{
    pdt::identity_fixture srv("v_srv");
    pdt::identity_fixture cli("v_cli");
    pdt::identity_fixture other("v_other");   // a 3rd identity for the wrong-digest pin

    constexpr int k_iterations = 100;

    SECTION("cross-pinned: accepts and completes")
    {
        int ok = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
            pdt::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; });
            REQUIRE(l.dialed != nullptr);
            REQUIRE(l.accepted != nullptr);
            ++ok;
        }
        REQUIRE(ok == k_iterations);
    }

    SECTION("server pins the WRONG client digest: fail closed (no channel)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, other.digest), pdt::pin_one(cli, srv.digest));
            pdt::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); },
                            std::chrono::milliseconds{1500});
            REQUIRE(l.accepted == nullptr);
            REQUIRE(l.dialed == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }

    SECTION("client pins the WRONG server digest: fail closed (no channel)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, other.digest));
            pdt::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); },
                            std::chrono::milliseconds{1500});
            REQUIRE(l.dialed == nullptr);
            REQUIRE(l.accepted == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }

    SECTION("empty (no-pin) policy on the server: fail closed (accepts nothing)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_none(srv), pdt::pin_one(cli, srv.digest));
            pdt::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); },
                            std::chrono::milliseconds{1500});
            REQUIRE(l.accepted == nullptr);
            REQUIRE(l.dialed == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }
}

TEST_CASE("dtls.handshake_loss: the handshake completes through a lossy relay, looped",
          "[dtls][handshake_loss]")
{
    pdt::identity_fixture srv("loss_srv");
    pdt::identity_fixture cli("loss_cli");

    constexpr int k_iterations = 100;
    int completed = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
        // Drop the first ClientHello, duplicate the cookie'd retry, then pass — OpenSSL
        // retransmit (NOT the ARQ) must recover. Subsequent flights pass.
        l.link->script = {pdt::action::drop, pdt::action::duplicate};
        pdt::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; });
        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

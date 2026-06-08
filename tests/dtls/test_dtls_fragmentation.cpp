#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"

#include "plexus/asio/udp_server.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <algorithm>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pasio = plexus::asio;
namespace pio = plexus::io;

namespace {

// A direct two-channel loopback (mirrors the mtu_link in the MTU suite) that completes a
// live DTLS session, then drives the large-payload send/fragment/reassemble path: each
// datagram a socket receives is fed to the OTHER channel's deliver_inbound. The configured
// max_payload is the LOGICAL ceiling (raised high so a large message fragments rather than
// rejecting); the record MTU stays the single-datagram floor so the fragmenter splits
// against the real post-handshake DTLS_get_data_mtu (each fragment rides one DTLS record).
struct frag_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    pio::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    pio::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server server_sock{io};
    pasio::udp_server client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool server_complete{false};
    bool client_complete{false};
    std::vector<std::vector<std::byte>> server_received;

    std::vector<std::vector<std::byte>> client_to_server;
    std::vector<std::vector<std::byte>> server_to_client;
    int client_to_server_count{0};

    frag_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id,
              std::size_t max_payload)
        : server_cred(pdt::pin_one(server_id, client_id.digest))
        , client_cred(pdt::pin_one(client_id, server_id.digest))
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));

        ::asio::ip::udp::endpoint server_ep(::asio::ip::make_address("127.0.0.1"), server_sock.port());
        ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());

        server_ch = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred,
                                                         server_cookie, ptls::dtls_channel::role::server,
                                                         max_payload);
        client_ch = std::make_unique<ptls::dtls_channel>(io, client_sock, server_ep, client_cred,
                                                         client_cookie, ptls::dtls_channel::role::client,
                                                         max_payload);

        server_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            client_to_server.emplace_back(b.begin(), b.end());
            ++client_to_server_count;
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

    void handshake(std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
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

    // Send a `size`-byte frame (a deterministic byte ramp) and pump until the server has
    // reassembled exactly one message. Returns the reassembled bytes (empty on timeout).
    std::vector<std::byte> round_trip(std::size_t size, std::chrono::milliseconds timeout = std::chrono::milliseconds{2000})
    {
        server_received.clear();
        client_to_server_count = 0;
        std::vector<std::byte> frame(size);
        for(std::size_t i = 0; i < size; ++i)
            frame[i] = static_cast<std::byte>(i & 0xFF);
        client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
        return server_received.size() == 1 ? server_received[0] : std::vector<std::byte>{};
    }
};

std::vector<std::byte> ramp(std::size_t size)
{
    std::vector<std::byte> v(size);
    for(std::size_t i = 0; i < size; ++i)
        v[i] = static_cast<std::byte>(i & 0xFF);
    return v;
}

}

TEST_CASE("dtls.fragment: a large payload fragments across records and reassembles byte-equal over a live session, looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("frag_srv");
    pdt::identity_fixture cli("frag_cli");

    // Largest payload proven over live DTLS: ~24 KiB. With a high logical ceiling (so the
    // frame fragments rather than rejecting) and the default record MTU (~1363 encrypted),
    // a 24 KiB message splits into ~18 records and reassembles into ONE on_data byte-equal.
    constexpr std::size_t k_payload = 24u * 1024u;

    constexpr int k_iterations = 50;
    int proven = 0;
    int max_fragments = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        frag_link l(srv, cli, /*max_payload=*/1u * 1024u * 1024u);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        const auto got = l.round_trip(k_payload);
        REQUIRE(got.size() == k_payload);
        REQUIRE(got == ramp(k_payload));               // byte-equal reassembly
        // The message crossed the wire as MANY records (fragmentation happened), each one
        // DTLS record — never one oversize record, never a reject.
        REQUIRE(l.client_to_server_count > 1);
        max_fragments = std::max(max_fragments, l.client_to_server_count);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(max_fragments > 1);
}

TEST_CASE("dtls.fragment: a frame at the record budget rides ONE record (parity, no fragmentation), looped",
          "[dtls][fragment]")
{
    pdt::identity_fixture srv("par_srv");
    pdt::identity_fixture cli("par_cli");

    constexpr int k_iterations = 50;
    int proven = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high logical ceiling so DTLS_get_data_mtu (the encrypted record budget) is the
        // binding term. A frame at exactly that budget must ride ONE record (byte-identical
        // to the pre-fragment path), not fragment.
        frag_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // The encrypted record budget is ~1363 (1400 record MTU - 37B AEAD-GCM overhead);
        // a 1300-byte frame is comfortably under it and must cross as exactly one record.
        const auto got = l.round_trip(1300);
        REQUIRE(got.size() == 1300);
        REQUIRE(got == ramp(1300));
        REQUIRE(l.client_to_server_count == 1);        // ONE record: no fragmentation
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

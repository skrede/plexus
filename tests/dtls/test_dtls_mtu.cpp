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
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pasio = plexus::asio;
namespace pio = plexus::io;

namespace {

// A direct two-channel loopback (mirrors the channel_link in the handshake suite)
// that completes a live DTLS session, then drives the send/MTU path: each datagram a
// socket receives is fed to the OTHER channel's deliver_inbound. The configurable
// max_payload lets a section choose whether the configured cap or DTLS_get_data_mtu
// binds in the min(configured_cap, DTLS_get_data_mtu) reject.
struct mtu_link
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    plexus::io::security::cookie_secret server_cookie{ptls::make_cookie_secret()};
    plexus::io::security::cookie_secret client_cookie{ptls::make_cookie_secret()};
    pasio::udp_server server_sock{io};
    pasio::udp_server client_sock{io};

    std::unique_ptr<ptls::dtls_channel> server_ch;
    std::unique_ptr<ptls::dtls_channel> client_ch;

    bool server_complete{false};
    bool client_complete{false};
    std::vector<std::vector<std::byte>> server_received;
    bool client_too_large{false};

    std::vector<std::vector<std::byte>> client_to_server;
    std::vector<std::vector<std::byte>> server_to_client;

    mtu_link(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id,
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
        });
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) {
            server_to_client.emplace_back(b.begin(), b.end());
        });

        server_ch->on_external_complete([this] { server_complete = true; });
        client_ch->on_external_complete([this] { client_complete = true; });
        server_ch->on_data([this](std::span<const std::byte> d) { server_received.emplace_back(d.begin(), d.end()); });
        client_ch->on_error([this](pio::io_error e) { if(e == pio::io_error::message_too_large) client_too_large = true; });
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

    // Send a frame of `size` bytes and pump until the server receives it OR an oversize
    // error fired. Returns true if the server received exactly one datagram of `size`.
    bool send_and_deliver(std::size_t size, std::chrono::milliseconds timeout = std::chrono::milliseconds{1000})
    {
        server_received.clear();
        client_too_large = false;
        std::vector<std::byte> frame(size, std::byte{0xab});
        client_ch->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + timeout;
        while(server_received.empty() && !client_too_large && std::chrono::steady_clock::now() < bound)
        {
            io.poll();
            if(io.stopped())
                io.restart();
            pump_relays();
        }
        return server_received.size() == 1 && server_received[0].size() == size;
    }

    // Probe the largest accepted frame by binary search over [lo, hi]: accept is
    // monotone in size (every size <= cap is delivered, every size > cap rejected), so
    // the boundary is discovered behaviorally — the test never reaches into the
    // channel's internals (no test-only DTLS_get_data_mtu accessor). `lo` must be
    // accepted and `hi` rejected for the invariant to hold.
    std::size_t probe_max_accepted(std::size_t lo, std::size_t hi)
    {
        if(!send_and_deliver(lo))
            return 0;                                  // floor not accepted: window wrong
        std::size_t accepted = lo;
        std::size_t rejected = hi;
        while(rejected - accepted > 1)
        {
            const std::size_t mid = accepted + (rejected - accepted) / 2;
            if(send_and_deliver(mid))
                accepted = mid;
            else
                rejected = mid;
        }
        return accepted;
    }
};

}

TEST_CASE("dtls.mtu: a frame at the data-MTU is delivered, one byte over is rejected via message_too_large, looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("mtu_srv");
    pdt::identity_fixture cli("mtu_cli");

    constexpr int k_iterations = 100;
    int proven = 0;
    std::size_t observed_cap = 0;

    for(int i = 0; i < k_iterations; ++i)
    {
        // A high configured cap so DTLS_get_data_mtu (the encrypted-record budget) is
        // the binding term of min(configured_cap, DTLS_get_data_mtu) (R-1).
        mtu_link l(srv, cli, /*max_payload=*/100000);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // Find the exact accept/reject boundary. DTLS_get_data_mtu is the configured
        // record budget (1400) minus the DTLS 1.2 AEAD-GCM record overhead (13B header +
        // 8B explicit IV + 16B auth tag = 37B), so the accepted ceiling lands at ~1363
        // — proving the cap binds the encrypted record budget, not the plaintext size.
        const std::size_t cap = l.probe_max_accepted(100, 1400);
        REQUIRE(cap >= 1300);                          // the ~1363 encrypted data MTU (1400 - 37B overhead)
        REQUIRE(cap < 1400);                           // strictly below the configured budget (overhead subtracted)
        observed_cap = cap;

        // The boundary frame is delivered intact AS ONE datagram (no fragmentation):
        // the server got exactly one on_data of exactly `cap` bytes.
        REQUIRE(l.send_and_deliver(cap));

        // One byte over the cap is rejected at publish via on_error(message_too_large)
        // and NOTHING crosses the wire (the server receives no datagram for it).
        REQUIRE_FALSE(l.send_and_deliver(cap + 1));
        REQUIRE(l.client_too_large);
        REQUIRE(l.server_received.empty());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
    REQUIRE(observed_cap > 0);
}

TEST_CASE("dtls.mtu: a low configured cap binds the reject below the data-MTU, looped",
          "[dtls][mtu]")
{
    pdt::identity_fixture srv("cap_srv");
    pdt::identity_fixture cli("cap_cli");

    // A configured cap well BELOW DTLS_get_data_mtu so the configured term binds
    // min(configured_cap, DTLS_get_data_mtu) — proving the reject is the MIN of the two,
    // not DTLS_get_data_mtu alone.
    constexpr std::size_t k_cap = 200;

    constexpr int k_iterations = 100;
    int proven = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        mtu_link l(srv, cli, k_cap);
        l.handshake();
        REQUIRE(l.client_complete);
        REQUIRE(l.server_complete);

        // At the configured cap: delivered as one datagram.
        REQUIRE(l.send_and_deliver(k_cap));
        // One byte over the configured cap: rejected even though it is well under the
        // encrypted data MTU — the configured cap is the binding term.
        REQUIRE_FALSE(l.send_and_deliver(k_cap + 1));
        REQUIRE(l.client_too_large);
        REQUIRE(l.server_received.empty());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

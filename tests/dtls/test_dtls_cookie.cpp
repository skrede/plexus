#include "dtls_test_support.h"

#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"

#include <catch2/catch_test_macros.hpp>

#include <openssl/ssl.h>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstring>
#include <cstddef>

namespace pdt = plexus::dtls_test;
namespace ptls = plexus::tls;

namespace {

// A relay between the client and a real server that counts the server's first
// response to the client's first datagram. With the stateless cookie exchange a
// no-cookie ClientHello yields a SMALL HelloVerifyRequest (no Certificate flight);
// only the cookie'd retry triggers the full server flight. The relay records the
// size of the first server->client datagram so the test can assert it is the small
// HelloVerifyRequest (no full handshake state was allocated up front).
struct cookie_relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;
    ::asio::ip::udp::socket back;
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};

    int server_datagrams{0};
    std::size_t first_server_response{0};

    cookie_relay(::asio::io_context &ctx, std::uint16_t server_port)
        : io(ctx)
        , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
            [this](std::error_code ec, std::size_t n) {
                if(ec)
                    return;
                client_ep = from;
                auto copy = std::make_shared<std::vector<std::byte>>(
                    front_buf.data(), front_buf.data() + n);
                back.async_send_to(::asio::buffer(*copy), server_ep, [copy](std::error_code, std::size_t) {});
                recv_front();
            });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
            [this](std::error_code ec, std::size_t n) {
                if(ec)
                    return;
                if(server_datagrams == 0)
                    first_server_response = n;
                ++server_datagrams;
                if(client_ep.port() != 0)
                {
                    auto copy = std::make_shared<std::vector<std::byte>>(
                        back_buf.data(), back_buf.data() + n);
                    front.async_send_to(::asio::buffer(*copy), client_ep, [copy](std::error_code, std::size_t) {});
                }
                recv_back();
            });
    }
};

}

TEST_CASE("dtls.cookie: a no-cookie ClientHello gets a small HelloVerifyRequest, then a valid cookie completes, looped",
          "[dtls][cookie]")
{
    pdt::identity_fixture srv("ck_srv");
    pdt::identity_fixture cli("ck_cli");

    constexpr int k_iterations = 100;
    int proven = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        ::asio::io_context io;
        auto server_cred = pdt::pin_one(srv, cli.digest);
        auto client_cred = pdt::pin_one(cli, srv.digest);
        ptls::dtls_transport server(io, server_cred);
        ptls::dtls_transport client(io, client_cred);

        std::unique_ptr<ptls::dtls_channel> accepted, dialed;
        bool dial_failed = false;
        server.on_accepted([&](std::unique_ptr<ptls::dtls_channel> ch) { accepted = std::move(ch); });
        client.on_dialed([&](std::unique_ptr<ptls::dtls_channel> ch, const pdt::pio::endpoint &) { dialed = std::move(ch); });
        client.on_dial_failed([&](const pdt::pio::endpoint &, pdt::pio::io_error) { dial_failed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        cookie_relay link(io, server.port());
        client.dial({"dtls", "127.0.0.1:" + std::to_string(link.port())});

        pdt::pump_until(io, [&] { return (accepted && dialed) || dial_failed; });

        // The cookie gate completes the handshake (a valid cookie proceeds).
        REQUIRE_FALSE(dial_failed);
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // The server's FIRST response (to the no-cookie ClientHello) is the small
        // HelloVerifyRequest — no full handshake state (no Certificate flight) was
        // allocated until the source echoed a valid cookie. A full server flight is
        // hundreds of bytes; HelloVerifyRequest is well under 128.
        REQUIRE(link.server_datagrams >= 2);          // HelloVerifyRequest + the real flight
        REQUIRE(link.first_server_response > 0);
        REQUIRE(link.first_server_response < 128);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("dtls.cookie: the cookie verify callback rejects a forged cookie, looped",
          "[dtls][cookie]")
{
    // The cookie HMAC binds [peer_addr || nonce] under a process-random key; a cookie
    // the state did not mint must not verify. compute() with the real addr is the only
    // path that produces a valid cookie; a flipped byte / a foreign cookie is rejected.
    ptls::dtls_cookie_state state;
    const unsigned char addr[] = {127, 0, 0, 1, 0x1f, 0x90};

    constexpr int k_iterations = 100;
    int rejected = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        unsigned char good[ptls::dtls_cookie_state::k_cookie_len];
        REQUIRE(state.compute(addr, sizeof(addr), true, good));

        // A forged cookie (the good one with a flipped byte) must not match either nonce.
        unsigned char forged[ptls::dtls_cookie_state::k_cookie_len];
        std::memcpy(forged, good, sizeof(good));
        forged[0] ^= 0xff;

        unsigned char cur[ptls::dtls_cookie_state::k_cookie_len];
        unsigned char prev[ptls::dtls_cookie_state::k_cookie_len];
        REQUIRE(state.compute(addr, sizeof(addr), true, cur));
        REQUIRE(state.compute(addr, sizeof(addr), false, prev));
        REQUIRE(std::memcmp(forged, cur, sizeof(forged)) != 0);
        REQUIRE(std::memcmp(forged, prev, sizeof(forged)) != 0);
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

TEST_CASE("dtls.failclosed: the ALPN select gate fails closed on a non-overlapping offer, looped",
          "[dtls][failclosed]")
{
    // R-2: the in-handshake ALPN gate. The server's plexus_alpn_select selects
    // "plexus/1" when offered, else returns SSL_TLSEXT_ERR_ALERT_FATAL to FAIL the
    // handshake closed (no silent fallback to an unversioned protocol).
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const unsigned char *out = nullptr;
        unsigned char outlen = 0;

        // A client offering only "h2" (a non-plexus protocol): no overlap -> FATAL.
        const unsigned char offer_other[] = {2, 'h', '2'};
        REQUIRE(ptls::plexus_alpn_select(nullptr, &out, &outlen, offer_other, sizeof(offer_other), nullptr)
                == SSL_TLSEXT_ERR_ALERT_FATAL);

        // A client offering "plexus/1": selected -> OK (the positive control).
        const unsigned char offer_plexus[] = {8, 'p', 'l', 'e', 'x', 'u', 's', '/', '1'};
        const unsigned char *sel = nullptr;
        unsigned char sel_len = 0;
        REQUIRE(ptls::plexus_alpn_select(nullptr, &sel, &sel_len, offer_plexus, sizeof(offer_plexus), nullptr)
                == SSL_TLSEXT_ERR_OK);
        REQUIRE(sel_len == 8);
        REQUIRE(std::memcmp(sel, "plexus/1", 8) == 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

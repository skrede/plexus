#include "test_dtls_cookie_common.h"

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

    std::uint16_t port() const
    {
        return front.local_endpoint().port();
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
                                 [this](std::error_code ec, std::size_t n)
                                 {
                                     if(ec)
                                         return;
                                     client_ep = from;
                                     auto copy = std::make_shared<std::vector<std::byte>>(front_buf.data(), front_buf.data() + n);
                                     back.async_send_to(::asio::buffer(*copy), server_ep, [copy](std::error_code, std::size_t) {});
                                     recv_front();
                                 });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
                                [this](std::error_code ec, std::size_t n)
                                {
                                    if(ec)
                                        return;
                                    if(server_datagrams == 0)
                                        first_server_response = n;
                                    ++server_datagrams;
                                    if(client_ep.port() != 0)
                                    {
                                        auto copy = std::make_shared<std::vector<std::byte>>(back_buf.data(), back_buf.data() + n);
                                        front.async_send_to(::asio::buffer(*copy), client_ep, [copy](std::error_code, std::size_t) {});
                                    }
                                    recv_back();
                                });
    }
};

}

TEST_CASE("dtls.cookie: a no-cookie ClientHello gets a small HelloVerifyRequest, then a valid "
          "cookie completes, looped",
          "[dtls][cookie]")
{
    pdt::identity_fixture srv("ck_srv");
    pdt::identity_fixture cli("ck_cli");

    constexpr int k_iterations = 100;
    int proven                 = 0;
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
        REQUIRE(link.server_datagrams >= 2); // HelloVerifyRequest + the real flight
        REQUIRE(link.first_server_response > 0);
        REQUIRE(link.first_server_response < 128);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

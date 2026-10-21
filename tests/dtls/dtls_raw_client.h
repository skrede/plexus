#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RAW_CLIENT_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RAW_CLIENT_H

#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/asio/udp_server.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/udp.hpp>
#include <asio/io_context.hpp>

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::dtls_test {

// A server dtls_channel handshaken against a raw OpenSSL client whose records go straight to deliver_inbound. Only a
// real peer can send the channel a close_notify; the channel has no verb that emits one.
struct raw_client
{
    ::asio::io_context io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    pio::security::cookie_secret cookie;
    plexus::asio::udp_server server_sock;
    plexus::asio::udp_server client_sock;
    std::unique_ptr<ptls::dtls_channel> channel;
    std::vector<std::vector<std::byte>> to_client;
    SSL *ssl;
    BIO *wire;
    bool complete;

    raw_client(const identity_fixture &server_id, const identity_fixture &client_id)
            : io()
            , server_cred(pin_one(server_id, client_id.digest))
            , client_cred(pin_one(client_id, server_id.digest))
            , cookie(ptls::make_cookie_secret())
            , server_sock(io)
            , client_sock(io)
            , channel()
            , to_client()
            , ssl(SSL_new(&client_cred.ssl_ctx()))
            , wire(nullptr)
            , complete(false)
    {
        server_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        client_sock.on_datagram([this](const ::asio::ip::udp::endpoint &, std::span<const std::byte> b) { to_client.emplace_back(b.begin(), b.end()); });
        const ::asio::ip::udp::endpoint client_ep(::asio::ip::make_address("127.0.0.1"), client_sock.port());
        channel = std::make_unique<ptls::dtls_channel>(io, server_sock, client_ep, server_cred, cookie, ptls::dtls_channel::role::server);
        channel->on_external_complete([this] { complete = true; });
        REQUIRE(ssl != nullptr);
        BIO *internal = nullptr;
        REQUIRE(BIO_new_bio_pair(&internal, 4096, &wire, 4096) == 1);
        SSL_set_bio(ssl, internal, internal);
        SSL_set_mtu(ssl, static_cast<long>(ptls::dtls_channel::default_max_payload));
        SSL_set_options(ssl, SSL_OP_NO_QUERY_MTU);
        SSL_set_connect_state(ssl);
    }

    raw_client(const raw_client &)            = delete;
    raw_client &operator=(const raw_client &) = delete;

    ~raw_client()
    {
        SSL_free(ssl);
        BIO_free(wire);
    }

    void handshake()
    {
        channel->start_handshake();
        const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(6);
        while((!complete || !SSL_is_init_finished(ssl)) && std::chrono::steady_clock::now() < bound)
        {
            (void)SSL_do_handshake(ssl);
            flush_to_server();
            pump();
            for(auto &dg : std::exchange(to_client, {}))
                BIO_write(wire, dg.data(), static_cast<std::int32_t>(dg.size()));
        }
        REQUIRE(complete);
    }

    void send_record(std::span<const std::byte> plaintext)
    {
        REQUIRE(SSL_write(ssl, plaintext.data(), static_cast<std::int32_t>(plaintext.size())) > 0);
        flush_to_server();
    }

    void send_whole(std::span<const std::byte> payload)
    {
        send_record(plexus::wire::wrap_udp(plexus::wire::udp_envelope_kind::best_effort, 0, payload));
    }

    void send_first_of_two(std::span<const std::byte> slice)
    {
        std::vector<std::byte> fragment;
        plexus::wire::wrap_udp_fragment_into(fragment, plexus::wire::udp_envelope_kind::best_effort, 0, 1, 0, 2, slice);
        send_record(fragment);
    }

    void send_close_notify()
    {
        (void)SSL_shutdown(ssl);
        flush_to_server();
    }

    void pump()
    {
        io.restart();
        io.poll();
    }

    void flush_to_server()
    {
        std::array<unsigned char, 2048> buf{};
        const auto size = static_cast<std::int32_t>(buf.size());
        for(std::int32_t n = BIO_read(wire, buf.data(), size); n > 0; n = BIO_read(wire, buf.data(), size))
            channel->deliver_inbound(std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)}));
    }
};

}

#endif

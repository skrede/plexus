#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_SILENT_PEER_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_SILENT_PEER_H

#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/fragmentation.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <asio/ip/udp.hpp>

#include <openssl/bio.h>
#include <openssl/ssl.h>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace plexus::dtls_test {

inline constexpr std::uint8_t k_server_hello         = 2;
inline constexpr std::uint8_t k_hello_verify_request = 3;

// A DTLS record header is 13 bytes, so a handshake record's message type is the byte after it (RFC 6347 section 4.1).
inline bool leads_with(const std::vector<std::byte> &datagram, std::uint8_t handshake_type)
{
    constexpr std::byte k_handshake_content{22};
    return datagram.size() > 13 && datagram[0] == k_handshake_content && datagram[13] == std::byte{handshake_type};
}

inline std::vector<std::vector<std::byte>> drain(::asio::ip::udp::socket &sock)
{
    std::vector<std::vector<std::byte>> taken;
    std::array<std::byte, 2048> buf{};
    ::asio::ip::udp::endpoint from;
    std::error_code ec;
    while(sock.available(ec) != 0 && !ec)
    {
        const std::size_t n = sock.receive_from(::asio::buffer(buf), from, 0, ec);
        if(!ec)
            taken.emplace_back(buf.data(), buf.data() + n);
    }
    return taken;
}

inline ptls::dtls_transport transport_with_ceiling(::asio::io_context &io, const ptls::tls_credential &cred, std::chrono::microseconds ceiling)
{
    return ptls::dtls_transport(io, cred, ptls::dtls_channel::default_max_payload, pio::global_default_max_message_bytes, pio::reassembly_memory_budget, ceiling);
}

// A bound loopback socket that never answers, so a handshake dialed to it only ever retransmits.
struct silent_socket
{
    ::asio::ip::udp::socket sock;

    explicit silent_socket(::asio::io_context &io)
            : sock(io, ::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), 0))
    {
    }

    pio::endpoint endpoint() const
    {
        return {"dtls", "127.0.0.1:" + std::to_string(sock.local_endpoint().port())};
    }

    std::int32_t take_received()
    {
        return static_cast<std::int32_t>(drain(sock).size());
    }
};

// A raw OpenSSL client that completes the cookie exchange with a transport, waits for the server's first flight and
// then never sends again; probe() resends its first ClientHello from the same source.
struct cookie_client
{
    ::asio::io_context &io;
    ptls::tls_credential cred;
    ::asio::ip::udp::socket sock;
    ::asio::ip::udp::endpoint server;
    std::unique_ptr<SSL, decltype(&SSL_free)> ssl;
    std::unique_ptr<BIO, decltype(&BIO_free_all)> wire;
    std::vector<std::byte> first_hello;

    cookie_client(::asio::io_context &ctx, const identity_fixture &self, const identity_fixture &peer, std::uint16_t server_port)
            : io(ctx)
            , cred(pin_one(self, peer.digest))
            , sock(io, ::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), 0))
            , server(::asio::ip::make_address("127.0.0.1"), server_port)
            , ssl(SSL_new(&cred.ssl_ctx()), &SSL_free)
            , wire(nullptr, &BIO_free_all)
            , first_hello()
    {
        REQUIRE(ssl != nullptr);
        BIO *internal = nullptr;
        BIO *external = nullptr;
        REQUIRE(BIO_new_bio_pair(&internal, 4096, &external, 4096) == 1);
        wire.reset(external);
        SSL_set_bio(ssl.get(), internal, internal);
        SSL_set_mtu(ssl.get(), static_cast<long>(ptls::dtls_channel::default_max_payload));
        SSL_set_options(ssl.get(), SSL_OP_NO_QUERY_MTU);
        SSL_set_connect_state(ssl.get());
    }

    void exchange_cookie()
    {
        (void)SSL_do_handshake(ssl.get());
        first_hello       = send_flight();
        const auto verify = await_datagrams();
        REQUIRE_FALSE(verify.empty());
        REQUIRE(leads_with(verify.front(), k_hello_verify_request));
        for(const auto &datagram : verify)
            BIO_write(wire.get(), datagram.data(), static_cast<std::int32_t>(datagram.size()));
        (void)SSL_do_handshake(ssl.get());
        send_flight();
        const auto flight = await_datagrams();
        REQUIRE_FALSE(flight.empty());
        REQUIRE(leads_with(flight.front(), k_server_hello));
    }

    void probe()
    {
        send(first_hello);
    }

    std::vector<std::vector<std::byte>> take()
    {
        return drain(sock);
    }

    std::int32_t count_led_by(std::uint8_t handshake_type, std::chrono::milliseconds window)
    {
        std::int32_t count = 0;
        const auto tally   = [&]
        {
            for(const auto &datagram : take())
                count += leads_with(datagram, handshake_type) ? 1 : 0;
            return false;
        };
        plexus::testing::pump_until(io, tally, window);
        return count;
    }

    bool await_handshake(std::uint8_t handshake_type, std::chrono::milliseconds bound)
    {
        bool seen       = false;
        const auto scan = [&]
        {
            for(const auto &datagram : take())
                seen = seen || leads_with(datagram, handshake_type);
            return seen;
        };
        return plexus::testing::pump_until(io, scan, bound);
    }

    std::vector<std::vector<std::byte>> await_datagrams()
    {
        plexus::testing::pump_until(io, [this] { return sock.available() != 0; }, std::chrono::seconds{2});
        return take();
    }

    std::vector<std::byte> send_flight()
    {
        std::vector<std::byte> first;
        std::array<unsigned char, 2048> buf{};
        const auto size = static_cast<std::int32_t>(buf.size());
        for(std::int32_t n = BIO_read(wire.get(), buf.data(), size); n > 0; n = BIO_read(wire.get(), buf.data(), size))
        {
            const auto bytes = std::as_bytes(std::span{buf.data(), static_cast<std::size_t>(n)});
            if(first.empty())
                first.assign(bytes.begin(), bytes.end());
            send(bytes);
        }
        return first;
    }

    void send(std::span<const std::byte> bytes)
    {
        sock.send_to(::asio::buffer(bytes.data(), bytes.size()), server);
    }
};

}

#endif

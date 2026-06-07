#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_TEST_SUPPORT_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_TEST_SUPPORT_H

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"
#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>
#include <asio/buffer.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <span>
#include <array>
#include <deque>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include <utility>
#include <cstddef>
#include <unistd.h>
#include <filesystem>
#include <system_error>

// Shared fixtures for the DTLS live tests: the EC P-256 self-signed cert+key
// generator (reused VERBATIM from test_tls_trust.cpp, the credential machinery is
// shared) with the SPKI digest recorded for cross-pinning, and the programmable
// loss-injecting relay (ported from tests/asio/test_udp_reliable_arq.cpp, but
// scripting RAW DTLS handshake-flight datagrams — DTLS records are self-framing,
// there is no ARQ envelope to filter on).
namespace plexus::dtls_test {

namespace ptls = plexus::tls;
namespace pio = plexus::io;

// The full 32-byte SHA-256 SPKI digest — the core cert_facts::spki_sha256 field type
// the spki_fingerprint extraction yields and the pin policy compares against.
using spki_digest = std::array<std::byte, 32>;

// ---- Ephemeral self-signed identity fixture -------------------------------

// An EC P-256 self-signed cert+key written to a fresh temp dir (key at 0600),
// with the cert's full-32-byte SPKI digest recorded for cross-pinning, and the
// subject CN recorded (R-OQ3: node_name == cert subject). The dir is removed on
// teardown. Distinct fixtures yield distinct SPKI digests.
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest digest{};
    std::string subject;

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path()
            / ("plexus_dtls_" + tag + "_" + std::to_string(::getpid())
               + "_" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::filesystem::create_directories(dir);
        cert_path = dir / "cert.pem";
        key_path = dir / "key.pem";
        generate(tag);
    }

    identity_fixture(const identity_fixture &) = delete;
    identity_fixture &operator=(const identity_fixture &) = delete;

    ~identity_fixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void generate(const std::string &tag)
    {
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(
            EVP_EC_gen("P-256"), &EVP_PKEY_free);
        REQUIRE(pkey);

        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), &X509_free);
        REQUIRE(cert);
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
        X509_set_pubkey(cert.get(), pkey.get());

        subject = "plexus-dtls-" + tag;
        X509_NAME *name = X509_get_subject_name(cert.get());
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char *>(subject.c_str()), -1, -1, 0);
        X509_set_issuer_name(cert.get(), name);
        REQUIRE(X509_sign(cert.get(), pkey.get(), EVP_sha256()) != 0);

        write_pem(cert.get(), pkey.get());
        std::filesystem::permissions(key_path,
            std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
            std::filesystem::perm_options::replace);

        auto d = ptls::spki_fingerprint(*cert.get());
        REQUIRE(d.has_value());
        digest = *d;
    }

    void write_pem(X509 *cert, EVP_PKEY *pkey)
    {
        FILE *cf = std::fopen(cert_path.c_str(), "wb");
        REQUIRE(cf != nullptr);
        REQUIRE(PEM_write_X509(cf, cert) == 1);
        std::fclose(cf);

        FILE *kf = std::fopen(key_path.c_str(), "wb");
        REQUIRE(kf != nullptr);
        REQUIRE(PEM_write_PrivateKey(kf, pkey, nullptr, nullptr, 0, nullptr, nullptr) == 1);
        std::fclose(kf);
    }
};

// Mint a DTLS credential for `self` whose verify policy pins exactly `peer_pin`.
inline ptls::tls_credential pin_one(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
        std::vector<spki_digest>{peer_pin});
    return ptls::load_dtls_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// Mint a DTLS credential for `self` whose verify policy pins an EMPTY set (the
// no-policy/accept-nothing fail-closed case).
inline ptls::tls_credential pin_none(const identity_fixture &self)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(std::vector<spki_digest>{});
    return ptls::load_dtls_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// ---- Programmable loss-injecting relay (RAW DTLS datagrams) ----------------

// What the relay does to one client->server datagram. The relay always passes
// server->client datagrams (the dialer must hear HelloVerifyRequest / the server
// flights); only the client->server direction is scripted.
enum class action { pass, drop, duplicate, hold };

// A programmable relay between a dialing client and a real server, forwarding RAW
// DTLS records both ways. For client->server datagrams it consults a scripted
// action queue (drop one, duplicate one, hold-then-release to reorder). Unlike the
// ARQ relay there is no envelope to peek — a DTLS record is opaque self-framed
// bytes, so EVERY client->server datagram is scripted in order.
struct relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front;       // faces the client
    ::asio::ip::udp::socket back;        // faces the server
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};

    std::deque<action> script;                            // consumed per client->server datagram
    std::vector<std::vector<std::byte>> held;             // held datagrams to release out of order
    int seen{0};

    relay(::asio::io_context &ctx, std::uint16_t server_port)
        : io(ctx)
        , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
        , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return front.local_endpoint().port(); }

    void send_to_server(std::span<const std::byte> dg)
    {
        auto copy = std::make_shared<std::vector<std::byte>>(dg.begin(), dg.end());
        back.async_send_to(::asio::buffer(*copy), server_ep,
            [copy](std::error_code, std::size_t) {});
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return;
                client_ep = from;
                handle_client(std::span<const std::byte>{front_buf.data(), n});
                recv_front();
            });
    }

    void handle_client(std::span<const std::byte> dg)
    {
        ++seen;
        action a = action::pass;
        if(!script.empty())
        {
            a = script.front();
            script.pop_front();
        }
        switch(a)
        {
        case action::pass:      send_to_server(dg); break;
        case action::drop:      break;                                  // lost: OpenSSL retransmits
        case action::duplicate: send_to_server(dg); send_to_server(dg); break;
        case action::hold:      held.emplace_back(dg.begin(), dg.end()); break;
        }
    }

    // Release every held datagram (out of order, after later ones already passed)
    // to exercise the receiver's reorder path; subsequent retransmits pass normally.
    void release_held()
    {
        for(auto &h : held)
            send_to_server(h);
        held.clear();
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return;
                if(client_ep.port() != 0)            // server->client always passes
                {
                    auto copy = std::make_shared<std::vector<std::byte>>(
                        back_buf.data(), back_buf.data() + n);
                    front.async_send_to(::asio::buffer(*copy), client_ep,
                        [copy](std::error_code, std::size_t) {});
                }
                recv_back();
            });
    }
};

// ---- io_context pumps ------------------------------------------------------

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred,
                std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

inline void settle(::asio::io_context &io, std::chrono::milliseconds window = std::chrono::milliseconds{40})
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

#endif

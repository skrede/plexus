// The gated real-TLS bounded-outbox hardening leg over asio loopback: the SECOND
// composition of the shared io::detail::stream_send_queue block (the plaintext
// asio_channel pins the first via test #225). A real mutual-TLS pair handshakes over a
// connected loopback socket; the server end NEVER reads, so the kernel send buffer fills
// and the encrypted async_write stalls — the dialed channel's userspace outbox then
// accumulates and the byte cap engages deterministically. A small cap (4 KiB) bounds the
// outbox; a stream of 1 KiB frames fills it, after which:
//   * congestion=drop sheds the overrun at the publisher (dropped_count advances), and
//   * congestion=block surfaces would_block (the stall edge) and sheds nothing.
// The outbox NEVER grows past the cap (backpressured() stays <= cap) — the same QoS edge
// the plaintext channel proves, here pinning that the TLS channel composes the SAME block
// rather than a divergent hand-rolled copy.

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/security/verify_policy.h"

#include "plexus/wire/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <utility>
#include <cstddef>
#include <optional>
#include <filesystem>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;
namespace wire = plexus::wire;

using spki_digest = std::array<std::byte, 32>;

namespace {

// An ephemeral EC P-256 self-signed identity written to a temp dir (key 0600); the SPKI
// digest is recorded for cross-pinning. Mirrors the tls_seam fixture.
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_tlsbp_" + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::filesystem::create_directories(dir);
        cert_path = dir / "cert.pem";
        key_path  = dir / "key.pem";
        generate();
    }

    identity_fixture(const identity_fixture &)            = delete;
    identity_fixture &operator=(const identity_fixture &) = delete;

    ~identity_fixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void generate()
    {
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(EVP_EC_gen("P-256"),
                                                                 &EVP_PKEY_free);
        REQUIRE(pkey);
        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), &X509_free);
        REQUIRE(cert);
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
        X509_set_pubkey(cert.get(), pkey.get());
        X509_NAME *name = X509_get_subject_name(cert.get());
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char *>("plexus-test"), -1, -1,
                                   0);
        X509_set_issuer_name(cert.get(), name);
        REQUIRE(X509_sign(cert.get(), pkey.get(), EVP_sha256()) != 0);
        write_pem(cert.get(), pkey.get());
        std::filesystem::permissions(
                key_path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
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

ptls::tls_credential make_cred(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

void pump(::asio::io_context &io, std::chrono::milliseconds window)
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

TEST_CASE("tls channel: the bounded outbox sheds under congestion=drop and stalls under block over "
          "real TLS",
          "[integration][tls][hardening]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    auto run = [&](pio::congestion mode)
    {
        // TWO io_contexts: the server end runs on its own context that is pumped DURING the
        // handshake but STOPPED once both ends are ready, so post-handshake the server's
        // ssl::stream never reads. The kernel buffers then fill and the client's encrypted
        // async_write stalls — the same never-reading-peer setup the plaintext #225 leg
        // uses (there a bare accepted socket that is never read; an ssl::stream cannot be
        // "adopted without reading," so the equivalent is to stop servicing its reads).
        ::asio::io_context client_io;
        ::asio::io_context server_io;

        ::asio::ip::tcp::acceptor acc{server_io,
                                      ::asio::ip::tcp::endpoint{::asio::ip::tcp::v4(), 0}};
        ::asio::ip::tcp::socket   raw_server{server_io};
        ::asio::ip::tcp::socket   raw_client{client_io};
        raw_client.connect(acc.local_endpoint());
        acc.accept(raw_server);

        auto server_cred = make_cred(server_id, client_id.digest);
        auto client_cred = make_cred(client_id, server_id.digest);

        constexpr std::size_t cap = 4096;
        ptls::tls_channel     server{
                server_io, std::move(raw_server), server_cred, {}, pio::congestion::block};
        ptls::tls_channel client{client_io,   std::move(raw_client),
                                 client_cred, wire::stream_inbound_config{},
                                 mode,        pio::egress_capacity::of_bytes(cap)};

        bool server_ready = false;
        bool client_ready = false;
        server.start_server_handshake([&] { server_ready = true; });
        client.start_client_handshake("", [&] { client_ready = true; });

        // Drive the mutual handshake to completion (both contexts pumped).
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while(!(server_ready && client_ready) && std::chrono::steady_clock::now() < bound)
        {
            server_io.poll();
            client_io.poll();
        }
        REQUIRE(server_ready);
        REQUIRE(client_ready);

        std::optional<pio::io_error> err;
        client.on_error([&](pio::io_error e) { err = e; });

        // Send 1 KiB frames until the byte cap engages. ONLY the client context is pumped:
        // the server stops reading, so its kernel/socket buffers fill and the client's
        // encrypted async_write stalls; the userspace outbox then accumulates to the cap.
        std::vector<std::byte> kib(1024, std::byte{0x5A});
        for(int i = 0; i < 8192; ++i)
        {
            client.send(kib);
            client_io.poll();
            REQUIRE(client.backpressured() <= cap); // NEVER grows past the cap
            if(mode == pio::congestion::drop_newest && client.dropped_count() > 0)
                break;
            if(mode == pio::congestion::block && err.has_value())
                break;
        }

        if(mode == pio::congestion::drop_newest)
        {
            REQUIRE(client.dropped_count() > 0); // the overrun was shed at the publisher
        }
        else
        {
            REQUIRE(err.has_value());
            REQUIRE(*err == pio::io_error::would_block); // block stalls, never grows
            REQUIRE(client.dropped_count() == 0);        // block sheds nothing
        }
        REQUIRE(client.backpressured() <= cap);

        client.close();
        server.close();
        pump(client_io, std::chrono::milliseconds(20));
        pump(server_io, std::chrono::milliseconds(20));
    };

    run(pio::congestion::drop_newest);
    run(pio::congestion::block);
}

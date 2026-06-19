// The TLS pending-dial teardown proof (asan): an in-flight TLS dial whose
// io_context (and owning transport) is destroyed mid-handshake leaks nothing. The half-
// open dial is owned by the transport's pending_dial_registry (transport-owned, not a
// self-owning readiness closure), so destroying the transport drops the registry and frees
// the half-open channel/socket/timer with no leak or use-after-free. This is a VERIFY-AND-
// PROVE rider: the single-owner fix is already landed; this asan leg pins it. Run under the
// build-asan tree (-fsanitize=address) so a residual self-owning shape surfaces as a leak.
//
// The dial targets a real listening TLS acceptor but the io_context is torn down BEFORE the
// mutual handshake completes (only the connect TCP leg is pumped, then everything is
// destroyed), so the channel is genuinely half-open at teardown. Looped so a per-iteration
// residue accumulates into an asan report.

#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/security/verify_policy.h"

#include "plexus/wire/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include <utility>
#include <cstddef>
#include <filesystem>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;
namespace wire = plexus::wire;

using spki_digest = std::array<std::byte, 32>;

namespace {

struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_tlsdt_" + tag + "_" + std::to_string(::getpid()) + "_" +
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

}

TEST_CASE("tls_dial_teardown: an io_context destroyed mid-handshake leaks nothing",
          "[integration][tls][teardown]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    // A standing server credential + a listener whose io_context OUTLIVES each dial attempt:
    // the dial targets a real listening acceptor so the TCP connect succeeds, but the dialer's
    // own io_context (and transport) is destroyed before the mutual handshake completes.
    for(int iter = 0; iter < 16; ++iter)
    {
        ::asio::io_context  server_io;
        auto                server_cred = make_cred(server_id, client_id.digest);
        ptls::tls_transport server{server_io, server_cred};
        server.listen(pio::endpoint{"tls", "127.0.0.1:0"});
        for(int i = 0; i < 50 && server.port() == 0; ++i)
            server_io.poll();
        const std::uint16_t port = server.port();
        REQUIRE(port != 0);

        {
            ::asio::io_context  client_io;
            auto                client_cred = make_cred(client_id, server_id.digest);
            ptls::tls_transport client{client_io, client_cred};
            client.dial(pio::endpoint{"tls", "127.0.0.1:" + std::to_string(port)});

            // Pump JUST enough for the TCP connect to fire and the handshake to begin, then
            // let everything tear down half-open: poll a few times, never to completion.
            for(int i = 0; i < 3; ++i)
            {
                client_io.poll_one();
                server_io.poll_one();
            }
            // client + client_io destroyed here, mid-handshake — the registry frees the
            // half-open dial. asan asserts no leak/UAF.
        }
    }
}

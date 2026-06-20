// over-limit: one cohesive TLS-over-mux E2E matrix; every cell drives the one shared
// ephemeral-cert + mux-face loopback-pair harness (whose identity + quintet-member preamble
// alone exceeds the file ceiling), so splitting the cells scatters that one harness.
#include "plexus/asio/all_backends_mux.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

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
#include <cstdint>
#include <utility>
#include <cstddef>
#include <optional>
#include <filesystem>

namespace pasio = plexus::asio;
namespace ptls  = plexus::tls;
namespace pio   = plexus::io;

// The full 32-byte SHA-256 SPKI digest — the core cert_facts::spki_sha256 field type.
using spki_digest = std::array<std::byte, 32>;

static_assert(
        plexus::io::transport_backend<pasio::all_backends_mux, plexus::muxify<pasio::asio_policy>>);

namespace {

// An EC P-256 self-signed identity (cert+key at 0600 in a fresh temp dir) with
// its SPKI digest recorded. The two TLS ends cross-pin each other's digest.
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_tls_mux_" + tag + "_" + std::to_string(::getpid()) + "_" +
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

ptls::tls_credential pin_one(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// A face of the mux: ONE multiplexing_transport over its OWN (unix, tcp, tls, udp,
// dtls) member quintet. Each face owns its members outright — the completion
// callbacks on a concrete member are single-slot, so the listen face and the dial
// face cannot share a member; a real two-node loopback gives each node its own set.
struct mux_face
{
    ::asio::io_context   &io;
    ptls::tls_credential  cred;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport   secure;
    // The datagram member stays inert on these tls/tcp routes — its socket is only ever
    // bound when a "udp" channel is actually dialed/accepted, which these faces never do.
    pasio::udp_transport datagram{io};
    // The secure-datagram (DTLS) member reuses this face's credential but stays inert on
    // the tls/tcp routes these faces exercise — its socket binds only on a "dtls" dial.
    ptls::dtls_transport    secure_datagram;
    pasio::all_backends_mux mux{local, remote, secure, datagram, secure_datagram};

    mux_face(::asio::io_context &ctx, ptls::tls_credential c)
            : io(ctx)
            , cred(std::move(c))
            , secure(io, cred)
            , secure_datagram(io, cred)
    {
    }
};

// A loopback pair of mux faces sharing one io_context: a listen face and a dial
// face, each a full (unix, tcp, tls) triple. The tls members cross-pin each
// other; the dial face drives a "tls" or "tcp" dial to the listen face's bound
// port. The accepted (listen-side) and dialed (dial-side) mux_channels are
// captured so the route + scheme-survival can be asserted on both ends.
struct mux_pair
{
    ::asio::io_context io;
    mux_face           listen_face;
    mux_face           dial_face;

    std::optional<pio::endpoint>                   dialed_ep;
    std::unique_ptr<pio::polymorphic_byte_channel> dialed;
    std::unique_ptr<pio::polymorphic_byte_channel> accepted;

    mux_pair(const identity_fixture &server_id, const identity_fixture &client_id)
            : listen_face(io, pin_one(server_id, client_id.digest))
            , dial_face(io, pin_one(client_id, server_id.digest))
    {
        listen_face.mux.on_accepted([this](std::unique_ptr<pio::polymorphic_byte_channel> ch)
                                    { accepted = std::move(ch); });
        dial_face.mux.on_dialed(
                [this](std::unique_ptr<pio::polymorphic_byte_channel> ch, const pio::endpoint &ep)
                {
                    dialed = std::move(ch);
                    dialed_ep.emplace(ep);
                });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

constexpr int k_iterations = 100;

}

TEST_CASE("tls mux: the selector classifies tls and tcp as remote, unix and inproc as local",
          "[tls][mux][select]")
{
    pio::transport_selector sel;
    const auto              reserved = pio::reliability_hint::unspecified;

    // "tls" is a REMOTE-tier scheme — so the locality confinement still EXCLUDES
    // it (a host-confined process|local topic never rides tls even though tls
    // encrypts). "tcp" is remote too; "unix"/"inproc" are the same-host local tier.
    REQUIRE(sel.select({"tls", "127.0.0.1:5000"}, reserved) == pio::transport_kind::remote);
    REQUIRE(sel.select({"tcp", "127.0.0.1:5000"}, reserved) == pio::transport_kind::remote);
    REQUIRE(sel.select({"unix", "/tmp/s"}, reserved) == pio::transport_kind::local);
    REQUIRE(sel.select({"inproc", "node-a"}, reserved) == pio::transport_kind::local);
}

TEST_CASE("tls mux: a tls dial routes to the secure member and the scheme survives the erasure, "
          "looped",
          "[tls][mux][route]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"tls", "127.0.0.1:0"});
        n.dial_face.mux.dial({"tls", "127.0.0.1:" + std::to_string(n.listen_face.secure.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);   // delivered POST-handshake via the secure member
        REQUIRE(n.accepted != nullptr); // accepted POST-handshake via the secure member
        // The "tls" scheme survives the type-erasure on BOTH ends.
        REQUIRE(n.dialed->remote_endpoint().scheme == "tls");
        REQUIRE(n.accepted->remote_endpoint().scheme == "tls");
        // The dialed endpoint is returned UNCHANGED (the correlation key).
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "tls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("tls mux: a tcp dial still routes to the plain-TCP member — tls and tcp coexist, looped",
          "[tls][mux][route]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // A mux that carries a secure member also routes a plain "tcp" dial to the
        // plaintext member — the two wire protocols coexist on one mux composition.
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"tcp", "127.0.0.1:0"});
        n.dial_face.mux.dial({"tcp", "127.0.0.1:" + std::to_string(n.listen_face.remote.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        REQUIRE(n.dialed->remote_endpoint().scheme == "tcp");
        REQUIRE(n.accepted->remote_endpoint().scheme == "tcp");
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "tcp");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

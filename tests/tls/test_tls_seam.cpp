#include "plexus/tls/tls_policy.h"
#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/tls/dtls_policy.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/detail/dtls_context.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/io/transport_backend.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ssl/context.hpp>
#include <asio/ssl/stream.hpp>
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

// The full 32-byte SHA-256 SPKI digest — the core cert_facts::spki_sha256 field type.
using spki_digest = std::array<std::byte, 32>;

static_assert(plexus::io::byte_channel<ptls::tls_channel>);
static_assert(plexus::io::transport_backend<ptls::tls_transport, ptls::tls_policy>);
static_assert(plexus::Policy<ptls::tls_policy>);

// The DTLS seam pieces: the dtls_channel byte_channel surface and the dtls_policy
// bundle satisfy the same compile-time seams as their TLS analogs.
static_assert(plexus::io::byte_channel<ptls::dtls_channel>);
static_assert(plexus::Policy<ptls::dtls_policy>);

namespace {

// ---- COMDAT-fold proof (Task-1 runtime gate, retained) --------------------

// Instantiate an asio::ssl::context + ssl::stream over a real io_context in the
// same link unit that folds the OpenSSL-touching OBJECT TU. A clean exit is the
// runtime proof the OBJECT-into-consumer wiring is COMDAT-safe (a second asio
// archive would collide at first io_context use — a green build alone does NOT
// prove it).
void comdat_probe()
{
    ::asio::io_context                           io;
    ::asio::ssl::context                         ctx(::asio::ssl::context::tls);
    ::asio::ssl::stream<::asio::ip::tcp::socket> stream(io, ctx);
    REQUIRE(stream.native_handle() != nullptr);
    io.poll();
}

// ---- Ephemeral self-signed identity fixture -------------------------------

struct identity
{
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};
};

// Generate an EC P-256 self-signed cert+key, write them to a fresh temp dir
// (key at 0600), and record the cert's SPKI digest for cross-pinning. The two
// ends pin each other's digest. The dir is removed on identity teardown.
struct identity_fixture
{
    std::filesystem::path dir;
    identity              id;

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_tls_" + tag + "_" + std::to_string(::getpid()) + "_" +
                 std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::filesystem::create_directories(dir);
        id.cert_path = dir / "cert.pem";
        id.key_path  = dir / "key.pem";
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
        std::filesystem::permissions(id.key_path,
                                     std::filesystem::perms::owner_read |
                                             std::filesystem::perms::owner_write,
                                     std::filesystem::perm_options::replace);

        auto d = ptls::spki_fingerprint(*cert.get());
        REQUIRE(d.has_value());
        id.digest = *d;
    }

    void write_pem(X509 *cert, EVP_PKEY *pkey)
    {
        FILE *cf = std::fopen(id.cert_path.c_str(), "wb");
        REQUIRE(cf != nullptr);
        REQUIRE(PEM_write_X509(cf, cert) == 1);
        std::fclose(cf);

        FILE *kf = std::fopen(id.key_path.c_str(), "wb");
        REQUIRE(kf != nullptr);
        REQUIRE(PEM_write_PrivateKey(kf, pkey, nullptr, nullptr, 0, nullptr, nullptr) == 1);
        std::fclose(kf);
    }
};

ptls::tls_credential make_cred(const identity &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// A DIALED loopback mutual-TLS link: a fresh io_context + a server-side and a
// client-side tls_transport (each with its own cross-pinning credential) per
// instance. listen("tls","127.0.0.1:0") then dial the bound port; the accepted
// (server-handshaking) channel lands via on_accepted, the dialed (client-
// handshaking) channel lands via on_dialed ONLY post-handshake.
struct dial_tls_link
{
    ::asio::io_context   io;
    ptls::tls_credential server_cred;
    ptls::tls_credential client_cred;
    ptls::tls_transport  server{io, server_cred};
    ptls::tls_transport  client{io, client_cred};

    std::unique_ptr<ptls::tls_channel> accepted; // server end
    std::unique_ptr<ptls::tls_channel> dialed;   // client end
    bool                               dial_failed{false};

    std::vector<std::vector<std::byte>> server_received;

    dial_tls_link(const identity &server_id, const identity &client_id)
            : server_cred(make_cred(server_id, client_id.digest))
            , client_cred(make_cred(client_id, server_id.digest))
    {
        server.on_accepted(
                [this](std::unique_ptr<ptls::tls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> d)
                                      { server_received.emplace_back(d.begin(), d.end()); });
                });
        client.on_dialed([this](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.on_dial_failed([this](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        server.listen({"tls", "127.0.0.1:0"});
        client.dial({"tls", "127.0.0.1:" + std::to_string(server.port())});
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

std::vector<std::byte> make_frame(const std::string &payload)
{
    plexus::wire::frame_header hdr{.type         = plexus::wire::msg_type::unidirectional,
                                   .flags        = 0,
                                   .session_id   = 1,
                                   .timestamp_ns = 0,
                                   .payload_len  = 0};
    auto                       bytes = reinterpret_cast<const std::byte *>(payload.data());
    return plexus::wire::encode_frame(hdr, std::span<const std::byte>{bytes, payload.size()});
}

}

TEST_CASE("tls seam: asio::ssl + OpenSSL TU links and runs without a COMDAT-fold crash",
          "[tls][seam]")
{
    comdat_probe();
    plexus::tls::tls_credential cred;
    REQUIRE_FALSE(cred.valid());
}

TEST_CASE("dtls seam: share_context up_refs the SSL_CTX once and returns an owning handle",
          "[tls][seam][dtls]")
{
    identity_fixture self_id("dtls_share");
    auto             cred = make_cred(self_id.id, self_id.id.digest);
    REQUIRE(cred.valid());

    {
        auto shared = ptls::detail::share_dtls_context(cred);
        REQUIRE(shared != nullptr); // a live, refcounted SSL_CTX*
        REQUIRE(reinterpret_cast<void *>(&cred.ssl_ctx()) ==
                reinterpret_cast<void *>(shared.get()));
    } // the shared handle's ref is released here

    // The credential's own ref survives the shared handle's release — the up_ref/
    // free accounting balanced (a double-free or under-ref would have crashed/freed).
    REQUIRE(cred.valid());
    REQUIRE_NOTHROW(ptls::detail::share_dtls_context(cred));

    // A default-constructed credential has no SSL_CTX: share_dtls_context fails closed.
    plexus::tls::tls_credential empty;
    REQUIRE_THROWS(ptls::detail::share_dtls_context(empty));
}

TEST_CASE("dtls seam: load_dtls_credential builds a valid DTLS SSL_CTX", "[tls][seam][dtls]")
{
    identity_fixture self_id("dtls_cred");
    auto             policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{self_id.id.digest});

    auto cred = ptls::load_dtls_credential(self_id.id.cert_path.string(),
                                           self_id.id.key_path.string(), policy);
    REQUIRE(cred.valid());

    // The DTLS ctx shares like the TLS one (the up_ref/free accounting is the same).
    REQUIRE_NOTHROW(ptls::detail::share_dtls_context(cred));

    // A missing verify policy fails closed.
    REQUIRE_THROWS(ptls::load_dtls_credential(self_id.id.cert_path.string(),
                                              self_id.id.key_path.string(), nullptr));
}

TEST_CASE("dtls seam: the cookie MAC is deterministic per nonce and binds the peer addr",
          "[tls][seam][dtls]")
{
    // The backend factory injects OpenSSL HMAC/RAND into the core cookie_secret; the
    // mint MAC binds [peer_addr || nonce] under the process-random key. Same addr =>
    // same minted MAC (deterministic, no rotation in this window); a cookie minted
    // for addr_a validates for addr_a but NOT for addr_b (the addr binding).
    auto            secret   = ptls::make_cookie_secret();
    const std::byte addr_a[] = {std::byte{127}, std::byte{0},    std::byte{0},
                                std::byte{1},   std::byte{0x1f}, std::byte{0x90}}; // 127.0.0.1:8080
    const std::byte addr_b[] = {std::byte{127}, std::byte{0},    std::byte{0},
                                std::byte{1},   std::byte{0x23}, std::byte{0x28}}; // 127.0.0.1:9000
    std::span<const std::byte> span_a{addr_a, sizeof(addr_a)};
    std::span<const std::byte> span_b{addr_b, sizeof(addr_b)};

    constexpr std::size_t       klen = pio::security::cookie_secret::k_cookie_len;
    std::array<std::byte, klen> mac_a1{};
    std::array<std::byte, klen> mac_a2{};
    std::array<std::byte, klen> mac_b{};

    REQUIRE(secret.mint(span_a, mac_a1));
    REQUIRE(secret.mint(span_a, mac_a2));
    REQUIRE(secret.mint(span_b, mac_b));

    // Same addr + same nonce => same MAC (deterministic); a different addr diverges.
    REQUIRE(mac_a1 == mac_a2);
    REQUIRE(mac_a1 != mac_b);

    // The minted cookie validates for its own addr but not for a different addr (the
    // peer-addr binding: a cookie issued to one source cannot be replayed by another).
    REQUIRE(secret.validate(span_a, mac_a1));
    REQUIRE_FALSE(secret.validate(span_b, mac_a1));
}

TEST_CASE(
        "tls transport: a DIALED pair completes a real mutual-TLS handshake over loopback, looped",
        "[tls][handshake]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_tls_link l(server_id.id, client_id.id);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });

        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr); // delivered POST-handshake only
        REQUIRE(l.dialed->remote_endpoint().scheme == "tls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("tls transport: a frame sent post-handshake reassembles byte-identical, looped",
          "[tls][handshake]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    const std::string payload = "encrypted-bytes-over-a-mutual-tls-channel";
    const auto        frame   = make_frame(payload);

    constexpr int k_iterations = 100;
    int           delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dial_tls_link l(server_id.id, client_id.id);
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });
        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);

        l.dialed->send(std::span<const std::byte>{frame});
        l.pump_until([&] { return !l.server_received.empty(); });

        REQUIRE(l.server_received.size() == 1);
        REQUIRE(l.server_received[0] == frame);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

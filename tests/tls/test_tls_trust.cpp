// over-limit: one cohesive TLS trust/pinning E2E matrix; every cell drives the one shared
// ephemeral-cert + mutual-TLS loopback harness (whose credential + live-handshake preamble
// alone exceeds the file ceiling), so splitting the cells scatters that one harness.
#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/io/endpoint.h"

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
#include <filesystem>

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

// The full 32-byte SHA-256 SPKI digest — the core cert_facts::spki_sha256 field type.
using spki_digest = std::array<std::byte, 32>;

namespace {

// ---- Ephemeral self-signed identity fixture -------------------------------

// An EC P-256 self-signed cert+key written to a fresh temp dir (key at 0600),
// with the cert's full-32-byte SPKI digest recorded for cross-pinning. The dir
// is removed on teardown. Distinct fixtures yield distinct SPKI digests — the
// reject cases need >=2 (and the wrong-digest case a 3rd) independent identities.
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_tls_trust_" + tag + "_" + std::to_string(::getpid()) + "_" +
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

// Mint a credential for `self` whose verify policy pins exactly `peer_pin`.
ptls::tls_credential pin_one(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// Mint a credential for `self` whose verify policy pins NOTHING (an empty
// allowlist) — fail-closed: it rejects every peer.
ptls::tls_credential pin_none(const identity_fixture &self)
{
    auto policy =
            std::make_shared<const pio::security::spki_pin_policy>(std::vector<spki_digest>{});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// A DIALED loopback mutual-TLS link parameterized on the two credentials. The
// accepter listens on "tls" "127.0.0.1:0"; the dialer dials the bound port. The
// accepted (server-handshaking) channel lands via on_accepted; the dialed
// (client-handshaking) channel lands via on_dialed ONLY post-handshake, so a
// verify reject on EITHER side leaves the corresponding end empty (fail-closed).
struct trust_link
{
    ::asio::io_context   io;
    ptls::tls_credential accepter_cred;
    ptls::tls_credential dialer_cred;
    ptls::tls_transport  accepter;
    ptls::tls_transport  dialer;

    std::unique_ptr<ptls::tls_channel> accepted;
    std::unique_ptr<ptls::tls_channel> dialed;
    bool                               dial_failed{false};
    bool                               dialed_errored{false};

    std::vector<std::vector<std::byte>> accepter_received;

    trust_link(ptls::tls_credential a, ptls::tls_credential d)
            : accepter_cred(std::move(a))
            , dialer_cred(std::move(d))
            , accepter(io, accepter_cred)
            , dialer(io, dialer_cred)
    {
        accepter.on_accepted(
                [this](std::unique_ptr<ptls::tls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> d)
                                      { accepter_received.emplace_back(d.begin(), d.end()); });
                });
        dialer.on_dialed(
                [this](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &)
                {
                    dialed = std::move(ch);
                    dialed->on_error([this](pio::io_error) { dialed_errored = true; });
                });
        dialer.on_dial_failed([this](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        accepter.listen({"tls", "127.0.0.1:0"});
        dialer.dial({"tls", "127.0.0.1:" + std::to_string(accepter.port())});
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    // A bounded settle window so a rejected handshake has time to surface its
    // failure (the reject path resolves without ever producing a channel).
    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(250))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    // Attempt a send over whatever the dialer holds (if anything) and pump, so a
    // data-level fail-closed check can confirm NOTHING reaches the accepter even
    // if the dialer's channel was transiently delivered before the accepter's
    // post-handshake reject alert tore it down.
    void try_send_then_settle(std::span<const std::byte> frame)
    {
        if(dialed)
            dialed->send(frame);
        settle();
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

constexpr int k_iterations = 100;

}

TEST_CASE(
        "tls trust: mutual cross-pinning lands both ends live and delivers byte-identical, looped",
        "[tls][trust][failclosed]")
{
    identity_fixture acc("acc");
    identity_fixture dia("dia");

    const std::string payload = "mutual-pin-accepts-and-the-frame-survives";
    const auto        frame   = make_frame(payload);

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        trust_link l(pin_one(acc, dia.digest), pin_one(dia, acc.digest));
        l.pump_until([&] { return (l.accepted && l.dialed) || l.dial_failed; });

        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.accepted != nullptr);
        REQUIRE(l.dialed != nullptr); // delivered POST-handshake only

        l.dialed->send(std::span<const std::byte>{frame});
        l.pump_until([&] { return !l.accepter_received.empty(); });
        REQUIRE(l.accepter_received.size() == 1);
        REQUIRE(l.accepter_received[0] == frame);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("tls trust: the accepter not pinning the dialer fails closed — no accepted channel, no "
          "data, looped",
          "[tls][trust][failclosed]")
{
    identity_fixture acc("acc");
    identity_fixture dia("dia");
    const auto       frame = make_frame("a-rejected-dialer-must-never-reach-the-accepter");

    int rejected = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The dialer correctly pins the accepter, but the accepter pins a THIRD
        // identity's digest instead of the dialer's — so the accepter (server)
        // rejects the dialer's (client) leaf.
        //
        // TLS 1.3 semantics: a server's rejection of the CLIENT cert is a
        // post-handshake alert — the client's handshake completes locally before
        // that alert arrives, so on_dialed may fire transiently. The fail-closed
        // guarantee is therefore asserted at the DATA boundary: the accepter
        // yields NO channel, the dialer's transient channel errors out, and NO
        // application byte ever crosses to the accepter.
        identity_fixture other("oth");
        trust_link       l(pin_one(acc, other.digest), pin_one(dia, acc.digest));
        l.pump_until([&] { return l.accepted || l.dialed || l.dial_failed; });
        l.try_send_then_settle(std::span<const std::byte>{frame});

        REQUIRE(l.accepted == nullptr);       // accepter-side reject: no accepted channel
        REQUIRE(l.accepter_received.empty()); // and NO data crosses to the accepter
        if(l.dialed)
            REQUIRE(l.dialed_errored); // the dialer's transient channel is dead-on-arrival
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

TEST_CASE("tls trust: the dialer not pinning the accepter fails closed — on_dialed never fires, "
          "looped",
          "[tls][trust][failclosed]")
{
    identity_fixture acc("acc");
    identity_fixture dia("dia");

    int rejected = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The accepter correctly pins the dialer, but the dialer pins a THIRD
        // identity's digest instead of the accepter's — proving the DIALER
        // verifies the server too (not just the server verifying the client).
        identity_fixture other("oth");
        trust_link       l(pin_one(acc, dia.digest), pin_one(dia, other.digest));
        l.pump_until([&] { return l.accepted || l.dialed || l.dial_failed; });
        l.settle();

        REQUIRE(l.dialed == nullptr); // the dialer NEVER reaches on_dialed (it verifies)
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

TEST_CASE("tls trust: a mismatched (wrong-digest) pin rejects — no channel, looped",
          "[tls][trust][failclosed]")
{
    identity_fixture acc("acc");
    identity_fixture dia("dia");

    int rejected = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // Both ends hold a WRONG digest (a third identity's) in their allowlist —
        // a mismatched pin on both sides. The dialer rejects the accepter's server
        // cert, which aborts the dialer's OWN handshake locally (a server-cert
        // reject is in-handshake for the client), so on_dialed never fires; the
        // accepter rejects too, so no accepted channel either.
        identity_fixture wrong("wrg");
        trust_link       l(pin_one(acc, wrong.digest), pin_one(dia, wrong.digest));
        l.pump_until([&] { return l.accepted || l.dialed || l.dial_failed; });
        l.settle();

        REQUIRE(l.accepted == nullptr);
        REQUIRE(l.dialed == nullptr);
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

TEST_CASE("tls trust: a credential with an empty (no-pin) policy rejects every peer, looped",
          "[tls][trust][failclosed]")
{
    identity_fixture acc("acc");
    identity_fixture dia("dia");
    const auto       frame = make_frame("an-empty-pin-policy-authorizes-no-one");

    int rejected = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The accepter pins NOTHING (empty allowlist) — fail-closed by default,
        // never accept-any. Even a dialer that correctly pins the accepter is
        // rejected because the accepter authorizes no one. Same TLS 1.3 dynamic
        // as the accepter-not-pinning case: the reject is asserted at the data
        // boundary (no accepted channel, no byte ever crosses).
        trust_link l(pin_none(acc), pin_one(dia, acc.digest));
        l.pump_until([&] { return l.accepted || l.dialed || l.dial_failed; });
        l.try_send_then_settle(std::span<const std::byte>{frame});

        REQUIRE(l.accepted == nullptr);
        REQUIRE(l.accepter_received.empty());
        if(l.dialed)
            REQUIRE(l.dialed_errored);
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_IDENTITY_FIXTURE_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_IDENTITY_FIXTURE_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"

#include <catch2/catch_test_macros.hpp>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdint>
#include <cstddef>
#include <unistd.h>
#include <filesystem>
#include <system_error>

// The EC P-256 self-signed cert+key generator (reused VERBATIM from test_tls_trust.cpp,
// the credential machinery is shared) with the SPKI digest recorded for cross-pinning.
namespace plexus::dtls_test {

namespace ptls = plexus::tls;
namespace pio  = plexus::io;

// The full 32-byte SHA-256 SPKI digest — the core cert_facts::spki_sha256 field type
// the spki_fingerprint extraction yields and the pin policy compares against.
using spki_digest = std::array<std::byte, 32>;

// An EC P-256 self-signed cert+key written to a fresh temp dir (key at 0600),
// with the cert's full-32-byte SPKI digest recorded for cross-pinning, and the
// subject CN recorded (R-OQ3: node_name == cert subject). The dir is removed on
// teardown. Distinct fixtures yield distinct SPKI digests.
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};
    std::string           subject;

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() / ("plexus_dtls_" + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::filesystem::create_directories(dir);
        cert_path = dir / "cert.pem";
        key_path  = dir / "key.pem";
        generate(tag);
    }

    identity_fixture(const identity_fixture &)            = delete;
    identity_fixture &operator=(const identity_fixture &) = delete;

    ~identity_fixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void generate(const std::string &tag)
    {
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(EVP_EC_gen("P-256"), &EVP_PKEY_free);
        REQUIRE(pkey);

        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), &X509_free);
        REQUIRE(cert);
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
        X509_set_pubkey(cert.get(), pkey.get());

        subject         = "plexus-dtls-" + tag;
        X509_NAME *name = X509_get_subject_name(cert.get());
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(subject.c_str()), -1, -1, 0);
        X509_set_issuer_name(cert.get(), name);
        REQUIRE(X509_sign(cert.get(), pkey.get(), EVP_sha256()) != 0);

        write_pem(cert.get(), pkey.get());
        std::filesystem::permissions(key_path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write, std::filesystem::perm_options::replace);

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
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(std::vector<spki_digest>{peer_pin});
    return ptls::load_dtls_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// Mint a DTLS credential for `self` whose verify policy pins an EMPTY set (the
// no-policy/accept-nothing fail-closed case).
inline ptls::tls_credential pin_none(const identity_fixture &self)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(std::vector<spki_digest>{});
    return ptls::load_dtls_credential(self.cert_path.string(), self.key_path.string(), policy);
}

}

#endif

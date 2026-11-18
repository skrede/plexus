#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_CHAIN_AUTHORITY_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_CHAIN_AUTHORITY_H

#include "dtls_identity_fixture.h"

#include "plexus/tls/tls_credential.h"

#include <catch2/catch_test_macros.hpp>

#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <memory>
#include <string>
#include <cstdint>

namespace plexus::dtls_test {

using key_ptr  = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using cert_ptr = std::unique_ptr<X509, decltype(&X509_free)>;

inline key_ptr rsa_key(std::uint32_t bits)
{
    key_ptr key(EVP_RSA_gen(bits), &EVP_PKEY_free);
    REQUIRE(key);
    return key;
}

// A null issuer self-signs.
inline cert_ptr issue_cert(const std::string &subject, EVP_PKEY &subject_key, X509 *issuer, EVP_PKEY &issuer_key, std::int64_t serial, bool ca)
{
    cert_ptr cert(X509_new(), &X509_free);
    REQUIRE(cert);
    X509_set_version(cert.get(), X509_VERSION_3);
    ASN1_INTEGER_set_int64(X509_get_serialNumber(cert.get()), serial);
    X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
    X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
    X509_set_pubkey(cert.get(), &subject_key);
    X509_NAME_add_entry_by_txt(X509_get_subject_name(cert.get()), "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>(subject.c_str()), -1, -1, 0);
    X509_set_issuer_name(cert.get(), X509_get_subject_name(issuer ? issuer : cert.get()));
    if(ca)
    {
        BASIC_CONSTRAINTS constraints{};
        constraints.ca = 1;
        REQUIRE(X509_add1_ext_i2d(cert.get(), NID_basic_constraints, &constraints, 1, X509V3_ADD_DEFAULT) == 1);
    }
    REQUIRE(X509_sign(cert.get(), &issuer_key, EVP_sha256()) != 0);
    return cert;
}

// An RSA-2048 root and intermediate that issue leaves, so a peer presents a leaf + intermediate chain the other end
// verifies up to the root it trusts, while the SPKI pin still decides the leaf.
struct chain_authority
{
    key_ptr root_key;
    key_ptr intermediate_key;
    cert_ptr root;
    cert_ptr intermediate;
    std::int64_t next_serial;

    chain_authority()
            : root_key(rsa_key(2048))
            , intermediate_key(rsa_key(2048))
            , root(issue_cert("plexus-dtls-root", *root_key, nullptr, *root_key, 1, true))
            , intermediate(issue_cert("plexus-dtls-intermediate", *intermediate_key, root.get(), *root_key, 2, true))
            , next_serial(3)
    {
    }

    void issue(identity_fixture &id)
    {
        key_ptr key   = rsa_key(2048);
        cert_ptr leaf = issue_cert(id.subject, *key, intermediate.get(), *intermediate_key, next_serial++, false);
        id.adopt(leaf.get(), key.get());
    }

    void present(const ptls::tls_credential &cred) const
    {
        REQUIRE(SSL_CTX_add1_chain_cert(&cred.ssl_ctx(), intermediate.get()) == 1);
    }

    void install(const ptls::tls_credential &cred) const
    {
        present(cred);
        REQUIRE(X509_STORE_add_cert(SSL_CTX_get_cert_store(&cred.ssl_ctx()), root.get()) == 1);
    }
};

}

#endif

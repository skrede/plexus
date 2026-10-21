#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TLS_RETIRE_DRAIN_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TLS_RETIRE_DRAIN_COMMON_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <filesystem>
#include <system_error>

namespace tls_retire_drain {

using spki_digest = std::array<std::byte, 32>;
using bio_handle  = std::unique_ptr<BIO, decltype(&BIO_free)>;

// An ephemeral self-signed P-256 identity on disk (key 0600), with its SPKI digest for the peer's pin.
struct identity
{
    std::filesystem::path dir;
    spki_digest digest;

    explicit identity(const std::string &tag)
            : dir(std::filesystem::temp_directory_path() / ("plexus_tlsretire_" + tag + "_" + std::to_string(plexus::testing::process_id())))
            , digest()
    {
        std::filesystem::create_directories(dir);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_EC_gen("P-256"), &EVP_PKEY_free);
        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), &X509_free);
        REQUIRE((key && cert));
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
        X509_set_pubkey(cert.get(), key.get());
        X509_NAME *name = X509_get_subject_name(cert.get());
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, reinterpret_cast<const unsigned char *>("plexus-test"), -1, -1, 0);
        X509_set_issuer_name(cert.get(), name);
        REQUIRE(X509_sign(cert.get(), key.get(), EVP_sha256()) != 0);
        write_pem(*cert, *key);
        digest = *plexus::tls::spki_fingerprint(*cert);
    }

    ~identity()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void write_pem(X509 &cert, EVP_PKEY &key) const
    {
        const bio_handle cert_out(BIO_new_file((dir / "cert.pem").string().c_str(), "wb"), &BIO_free);
        const bio_handle key_out(BIO_new_file((dir / "key.pem").string().c_str(), "wb"), &BIO_free);
        REQUIRE((cert_out && key_out));
        REQUIRE(PEM_write_bio_X509(cert_out.get(), &cert) == 1);
        REQUIRE(PEM_write_bio_PrivateKey(key_out.get(), &key, nullptr, nullptr, 0, nullptr, nullptr) == 1);
        std::filesystem::permissions(dir / "key.pem", std::filesystem::perms::owner_read | std::filesystem::perms::owner_write, std::filesystem::perm_options::replace);
    }

    plexus::tls::tls_credential credential(const spki_digest &peer) const
    {
        auto policy = std::make_shared<const plexus::io::security::spki_pin_policy>(std::vector<spki_digest>{peer});
        return plexus::tls::load_credential((dir / "cert.pem").string(), (dir / "key.pem").string(), policy);
    }

    plexus::tls::tls_credential dtls_credential(const spki_digest &peer) const
    {
        auto policy = std::make_shared<const plexus::io::security::spki_pin_policy>(std::vector<spki_digest>{peer});
        return plexus::tls::load_dtls_credential((dir / "cert.pem").string(), (dir / "key.pem").string(), policy);
    }
};

// A server and a client identity pinned to each other; the credentials outlive every channel built from them.
struct mutual_credentials
{
    identity server_id;
    identity client_id;
    plexus::tls::tls_credential server;
    plexus::tls::tls_credential client;

    mutual_credentials()
            : server_id("srv")
            , client_id("cli")
            , server(server_id.credential(client_id.digest))
            , client(client_id.credential(server_id.digest))
    {
    }
};

struct mutual_dtls_credentials
{
    identity server_id;
    identity client_id;
    plexus::tls::tls_credential server;
    plexus::tls::tls_credential client;

    mutual_dtls_credentials()
            : server_id("dsrv")
            , client_id("dcli")
            , server(server_id.dtls_credential(client_id.digest))
            , client(client_id.dtls_credential(server_id.digest))
    {
    }
};

struct tls_pair
{
    ::asio::io_context io;
    ::asio::ip::tcp::acceptor acc;
    ::asio::ip::tcp::socket raw_server;
    ::asio::ip::tcp::socket raw_client;

    tls_pair()
            : io()
            , acc(io, ::asio::ip::tcp::endpoint{::asio::ip::make_address("127.0.0.1"), 0})
            , raw_server(io)
            , raw_client(io)
    {
        raw_client.connect(acc.local_endpoint());
        acc.accept(raw_server);
    }
};

}

#endif

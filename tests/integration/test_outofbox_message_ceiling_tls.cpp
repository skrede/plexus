#include "test_outofbox_message_ceiling_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace outofbox_ceiling_fixture;

#ifdef PLEXUS_HAVE_TLS_OUTOFBOX
namespace ptls = plexus::tls;

namespace {

using spki_digest = std::array<std::byte, 32>;

// An EC P-256 self-signed cert+key in a fresh temp dir, with the SPKI digest recorded for
// cross-pinning (the shared TLS-fixture shape, inlined here for the out-of-box leg).
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest           digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path() /
                ("plexus_oob_" + tag + "_" +
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
                                   reinterpret_cast<const unsigned char *>("plexus-oob-tls"), -1,
                                   -1, 0);
        X509_set_issuer_name(cert.get(), name);
        REQUIRE(X509_sign(cert.get(), pkey.get(), EVP_sha256()) != 0);

        FILE *cf = std::fopen(cert_path.c_str(), "wb");
        REQUIRE(cf != nullptr);
        REQUIRE(PEM_write_X509(cf, cert.get()) == 1);
        std::fclose(cf);
        FILE *kf = std::fopen(key_path.c_str(), "wb");
        REQUIRE(kf != nullptr);
        REQUIRE(PEM_write_PrivateKey(kf, pkey.get(), nullptr, nullptr, 0, nullptr, nullptr) == 1);
        std::fclose(kf);
        std::filesystem::permissions(
                key_path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
                std::filesystem::perm_options::replace);

        auto d = ptls::spki_fingerprint(*cert.get());
        REQUIRE(d.has_value());
        digest = *d;
    }
};

ptls::tls_credential make_cred(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
            std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

}

TEST_CASE("outofbox: an 8 MiB message round-trips over mutual-TLS at shipped defaults, looped",
          "[outofbox][envelope8]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    const auto body  = ramp_payload(k_shipped_ceiling);
    const auto frame = ceiling_frame(body);

    constexpr int iterations = 2;
    int           proven     = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        auto               server_cred = make_cred(server_id, client_id.digest);
        auto               client_cred = make_cred(client_id, server_id.digest);
        // The credential is required; every SIZE/back-pressure knob keeps its shipped default.
        ptls::tls_transport server{io, server_cred};
        ptls::tls_transport client{io, client_cred};

        std::unique_ptr<ptls::tls_channel> accepted, dialed;
        std::vector<std::byte>             got;
        std::optional<wire::close_cause>   closed;
        bool                               dial_failed = false;
        server.on_accepted(
                [&](std::unique_ptr<ptls::tls_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d)
                                      { got.assign(d.begin(), d.end()); });
                    accepted->on_protocol_close([&](wire::close_cause c) { closed = c; });
                });
        client.on_dialed([&](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.on_dial_failed([&](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        server.listen({"tls", "127.0.0.1:0"});
        client.dial({"tls", "127.0.0.1:" + std::to_string(server.port())});

        pump_until(io, [&] { return (accepted && dialed) || dial_failed; }, ms{8000});
        REQUIRE_FALSE(dial_failed);
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        pump_until(io, [&] { return got.size() == frame.size() || closed.has_value(); });
        REQUIRE_FALSE(closed.has_value()); // the default receive ceiling admitted the 8 MiB frame
        REQUIRE(got.size() == frame.size());
        REQUIRE(equal_bytes(got, frame)); // byte-equal at the shipped ceiling, default caps
        ++proven;
    }
    REQUIRE(proven == iterations);
}
#endif

// The lifted single-message size envelope on the secure TLS stream. A 16 MB frame
// round-trips byte-equal over a live mutual-TLS loopback pair: the node per-MESSAGE
// receive ceiling + the aggregate reassembly budget + the send-side outbox byte cap are
// all raised through the tls_transport ctor (the node-options surface) so the encrypted
// 16 MB message is admitted at the receiver and held in the send queue under
// congestion=block. Looped in-body and re-run across process runs (a transport claim is
// never made from one run); a position ramp in the body catches a reorder/corruption.

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/stream_inbound.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

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
#include <cstddef>
#include <utility>
#include <optional>
#include <algorithm>
#include <filesystem>

namespace ptls = plexus::tls;
namespace pio = plexus::io;
namespace wire = plexus::wire;

using spki_digest = std::array<std::byte, 32>;

namespace {

// An EC P-256 self-signed cert+key in a fresh temp dir, with the SPKI digest recorded for
// cross-pinning (the same fixture shape the seam/trust suites use, inlined here).
struct identity_fixture
{
    std::filesystem::path dir;
    std::filesystem::path cert_path;
    std::filesystem::path key_path;
    spki_digest digest{};

    explicit identity_fixture(const std::string &tag)
    {
        dir = std::filesystem::temp_directory_path()
            / ("plexus_tlslarge_" + tag + "_" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::filesystem::create_directories(dir);
        cert_path = dir / "cert.pem";
        key_path = dir / "key.pem";
        generate();
    }

    identity_fixture(const identity_fixture &) = delete;
    identity_fixture &operator=(const identity_fixture &) = delete;

    ~identity_fixture()
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void generate()
    {
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> pkey(EVP_EC_gen("P-256"), &EVP_PKEY_free);
        REQUIRE(pkey);
        std::unique_ptr<X509, decltype(&X509_free)> cert(X509_new(), &X509_free);
        REQUIRE(cert);
        ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert.get()), 0);
        X509_gmtime_adj(X509_getm_notAfter(cert.get()), 3600);
        X509_set_pubkey(cert.get(), pkey.get());
        X509_NAME *name = X509_get_subject_name(cert.get());
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            reinterpret_cast<const unsigned char *>("plexus-tls-large"), -1, -1, 0);
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
        std::filesystem::permissions(key_path,
            std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
            std::filesystem::perm_options::replace);

        auto d = ptls::spki_fingerprint(*cert.get());
        REQUIRE(d.has_value());
        digest = *d;
    }
};

ptls::tls_credential make_cred(const identity_fixture &self, const spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(std::vector<spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

std::vector<std::byte> ramp_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

}

TEST_CASE("tls.large: a 16 MB single frame round-trips byte-identically over a live mutual-TLS loopback pair, looped",
          "[tls][envelope16]")
{
    identity_fixture server_id("srv");
    identity_fixture client_id("cli");

    constexpr std::size_t k_payload = 16u * 1024u * 1024u;
    constexpr std::size_t k_ceiling = 20u * 1024u * 1024u;
    constexpr std::size_t k_budget = 48u * 1024u * 1024u;
    constexpr std::size_t k_outbox = k_ceiling + 4u * 1024u * 1024u;

    const auto body = ramp_payload(k_payload);
    wire::frame_header hdr{};
    hdr.type = wire::msg_type::unidirectional;
    hdr.payload_len = body.size();
    const auto frame = wire::encode_frame(hdr, std::span<const std::byte>{body});

    constexpr int k_iterations = 2;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        auto server_cred = make_cred(server_id, client_id.digest);
        auto client_cred = make_cred(client_id, server_id.digest);
        // The node-options surface: raise the receive ceiling + aggregate budget + send outbox
        // for the 16 MB path. The intermediate ctor params keep their defaults.
        ptls::tls_transport server{io, server_cred, wire::stream_inbound_config{}, true,
                                   pio::congestion::block, k_outbox, {}, k_ceiling, k_budget};
        ptls::tls_transport client{io, client_cred, wire::stream_inbound_config{}, true,
                                   pio::congestion::block, k_outbox, {}, k_ceiling, k_budget};

        std::unique_ptr<ptls::tls_channel> accepted, dialed;
        std::vector<std::byte> got;
        std::optional<wire::close_cause> closed;
        bool dial_failed = false;
        server.on_accepted([&](std::unique_ptr<ptls::tls_channel> ch) {
            accepted = std::move(ch);
            accepted->on_data([&](std::span<const std::byte> d) { got.assign(d.begin(), d.end()); });
            accepted->on_protocol_close([&](wire::close_cause c) { closed = c; });
        });
        client.on_dialed([&](std::unique_ptr<ptls::tls_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.on_dial_failed([&](const pio::endpoint &, pio::io_error) { dial_failed = true; });

        server.listen({"tls", "127.0.0.1:0"});
        client.dial({"tls", "127.0.0.1:" + std::to_string(server.port())});

        auto handshake_bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!((accepted && dialed) || dial_failed) && std::chrono::steady_clock::now() < handshake_bound)
            io.poll();
        REQUIRE_FALSE(dial_failed);
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while(got.size() < frame.size() && !closed && std::chrono::steady_clock::now() < bound)
            io.poll();

        REQUIRE_FALSE(closed.has_value());          // the receive ceiling admitted the encrypted 16 MB frame
        REQUIRE(got.size() == frame.size());
        REQUIRE(got == frame);                       // byte-equal reassembly over the secure channel
        const auto delivered_body = std::span<const std::byte>{got}.subspan(wire::header_size);
        REQUIRE(std::equal(delivered_body.begin(), delivered_body.end(), body.begin()));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

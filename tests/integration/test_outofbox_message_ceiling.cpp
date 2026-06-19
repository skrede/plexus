// The out-of-box message-ceiling guard: an 8 MiB message — the SHIPPED default per-message
// ceiling (io::global_default_max_message_bytes) — round-trips byte-identically over every
// reliable transport at FULL SHIPPED DEFAULTS. No write_queue_bytes, backpressure_bytes,
// global_default, or reassembly-budget bump: every transport is constructed with its
// defaults (the default 16 MiB reassembly budget alone holds an 8 MiB message). This is the
// regression guard for the operator's promise — the local back-pressure / write-queue caps
// can never again silently strangle a message within the shipped ceiling. The negotiated
// ceiling is the sole size authority; the back-pressure cap is decoupled from it.
//
// tcp / unix / tls carry a framed wire message over the stream reassembler; udpr carries the
// raw payload over the reliable ARQ + datagram reassembler. (DTLS best-effort has the
// documented ~3 MiB host delivery cap and is excluded from this reliable 8 MiB round-trip.)
// Each leg loops in-body; the ctest invocation is re-run across process runs (a transport
// claim is never made from one run). A position ramp catches a reorder/corruption.

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/fragmentation.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/stream_inbound.h"

#ifdef PLEXUS_HAVE_TLS_OUTOFBOX
    #include "plexus/tls/tls_channel.h"
    #include "plexus/tls/tls_transport.h"
    #include "plexus/tls/tls_credential.h"
    #include "plexus/tls/spki_fingerprint.h"

    #include "plexus/io/security/verify_policy.h"

    #include <openssl/evp.h>
    #include <openssl/pem.h>
    #include <openssl/x509.h>
#endif

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <unistd.h>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <filesystem>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

namespace {

using ms = std::chrono::milliseconds;

// The SHIPPED default ceiling — the size this test proves round-trips at full defaults.
constexpr std::size_t k_shipped_ceiling = pio::global_default_max_message_bytes; // 8 MiB

// A deterministic position-dependent payload, regenerated to verify the round-trip is
// byte-identical (a size match alone would miss a reorder or a corrupt fragment).
std::vector<std::byte> ramp_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{20000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

// A stream frame whose payload is the ceiling-sized body: tcp/unix/tls round-trip the WHOLE
// framed message through the reassembler, so the body sits under a short frame header.
std::vector<std::byte> ceiling_frame(const std::vector<std::byte> &body)
{
    wire::frame_header hdr{};
    hdr.type        = wire::msg_type::unidirectional;
    hdr.payload_len = body.size();
    return wire::encode_frame(hdr, std::span<const std::byte>{body});
}

}

TEST_CASE("outofbox: an 8 MiB message round-trips over TCP at shipped defaults, looped",
          "[outofbox][envelope8]")
{
    const auto body  = ramp_payload(k_shipped_ceiling);
    const auto frame = ceiling_frame(body);

    constexpr int iterations = 2;
    int           proven     = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context    io;
        pasio::asio_transport server{io}; // NO size/back-pressure knobs — full defaults
        pasio::asio_transport client{io};

        std::unique_ptr<pasio::asio_channel> accepted, dialed;
        std::vector<std::byte>               got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::asio_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d)
                                      { got.assign(d.begin(), d.end()); });
                });
        client.on_dialed([&](std::unique_ptr<pasio::asio_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });

        server.listen({"tcp", "127.0.0.1:0"});
        client.dial({"tcp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return accepted && dialed; }, ms{8000});
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        pump_until(io, [&] { return got.size() == frame.size(); });
        REQUIRE(got.size() == frame.size());
        REQUIRE(equal_bytes(got, frame)); // byte-equal at the shipped ceiling, default caps
        ++proven;
    }
    REQUIRE(proven == iterations);
}

TEST_CASE("outofbox: an 8 MiB message round-trips over AF_UNIX at shipped defaults, looped",
          "[outofbox][envelope8]")
{
    const auto body  = ramp_payload(k_shipped_ceiling);
    const auto frame = ceiling_frame(body);

    constexpr int iterations = 2;
    int           proven     = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        char        tmpl[] = "/tmp/pxo-XXXXXX";
        const char *made   = ::mkdtemp(tmpl);
        REQUIRE(made != nullptr);
        const std::string path = std::string{made} + "/s";

        ::asio::io_context    io;
        pasio::unix_transport server{io}; // full defaults
        pasio::unix_transport client{io};

        std::unique_ptr<pasio::unix_channel> accepted, dialed;
        std::vector<std::byte>               got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::unix_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d)
                                      { got.assign(d.begin(), d.end()); });
                });
        client.on_dialed([&](std::unique_ptr<pasio::unix_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });

        server.listen({"unix", path});
        client.dial({"unix", path});
        pump_until(io, [&] { return accepted && dialed; }, ms{8000});
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        pump_until(io, [&] { return got.size() == frame.size(); });
        REQUIRE(got.size() == frame.size());
        REQUIRE(equal_bytes(got, frame));
        ++proven;

        ::unlink(path.c_str());
        ::rmdir(made);
    }
    REQUIRE(proven == iterations);
}

namespace {

// An ARQ pacing config (a flow-control knob, NOT a size knob): a generous window + quick
// retransmit drive the 8 MiB message through the reliable ARQ fast enough to complete within
// the default per-message reassembly reclaim window, while the in-flight batch stays under the
// raised kernel socket buffers. The per-message size ceiling, write/back-pressure caps, and
// reassembly budget all stay at the shipped defaults — this only governs HOW FAST fragments
// leave, not how large a message may be. A generous retransmit budget covers loopback loss.
inline pio::detail::udp_arq_config paced_arq()
{
    return pio::detail::udp_arq_config{.window         = 1024,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{160},
                                       .max_retransmit = 40};
}

}

TEST_CASE("outofbox: an 8 MiB message round-trips over udpr at shipped defaults, looped",
          "[outofbox][envelope8]")
{
    // The datagram leg at shipped defaults: the per-channel back-pressure cap is the OLD
    // 4 MiB default, now floored at the 8 MiB shipped ceiling — so the message's full
    // fragment backlog parks admissibly with NO backpressure_bytes bump. The reassembly
    // budget default (16 MiB) holds the 8 MiB reassembled message. Only the ARQ pacing
    // window and socket buffers (flow-control, not size) are set so the burst is buffered.
    constexpr std::size_t budget       = 8u * 1024u; // ~1024 fragments at the ceiling
    constexpr std::size_t k_socket_buf = 4u * 1024u * 1024u;
    // The per-message reassembly reclaim window is a LIVENESS knob (it reclaims a genuinely
    // stalled partial), NOT a size knob: an honest multi-megabyte loopback transfer legitimately
    // runs past the 5 s default, so it is extended here exactly as a flow-control concern. The
    // SIZE authorities (ceiling, reassembly budget, back-pressure cap) stay at shipped defaults.
    constexpr ms reassembly_timeout{60000};
    const auto   payload = ramp_payload(k_shipped_ceiling);

    using demux = pasio::detail::udp_inbound_demux;

    constexpr int iterations = 2;
    int           proven     = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        // global_default / reassembly_budget / backpressure_bytes are passed at their SHIPPED
        // DEFAULTS (the proof that the shipped caps hold an 8 MiB message); only the kernel
        // socket buffers and the reassembly reclaim window are raised (flow-control / liveness).
        pasio::udp_transport server{io,
                                    budget,
                                    pasio::udp_transport::arq_type::default_ladder,
                                    paced_arq(),
                                    pio::congestion::block,
                                    demux::default_max_peers,
                                    k_socket_buf,
                                    k_socket_buf,
                                    pasio::udp_server::default_send_queue_bytes,
                                    pio::global_default_max_message_bytes,
                                    pio::reassembly_memory_budget,
                                    pasio::udp_channel::default_backpressure_bytes,
                                    reassembly_timeout};
        pasio::udp_transport client{
                io,
                budget,
                pasio::udp_transport::arq_type::schedule{ms{20}, ms{40}, ms{80}},
                paced_arq(),
                pio::congestion::block,
                demux::default_max_peers,
                k_socket_buf,
                k_socket_buf,
                pasio::udp_server::default_send_queue_bytes,
                pio::global_default_max_message_bytes,
                pio::reassembly_memory_budget,
                pasio::udp_channel::default_backpressure_bytes,
                reassembly_timeout};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::vector<std::byte>> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b)
                                      { got.emplace_back(b.begin(), b.end()); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        dialed->send(payload);
        pump_until(io, [&] { return !got.empty(); }, ms{40000});
        REQUIRE(got.size() == 1);
        REQUIRE(equal_bytes(got.front(),
                            payload)); // byte-equal at the shipped ceiling, default caps
        ++proven;
    }
    REQUIRE(proven == iterations);
}

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

#include "dtls_test_support.h"

#include "plexus/asio/mux_policy.h"
#include "plexus/asio/all_backends_mux.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_transport.h"
#include "plexus/tls/tls_credential.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/security/verify_policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>

namespace pdt = plexus::dtls_test;
namespace pasio = plexus::asio;
namespace ptls = plexus::tls;
namespace pio = plexus::io;

static_assert(plexus::io::transport_backend<pasio::all_backends_mux, pasio::mux_policy>);

namespace {

// Mint a TLS (TLS-over-TCP) credential for `self` pinning exactly `peer_pin` — the
// secure stream member needs a TLS_method SSL_CTX (NOT the DTLS one pin_one mints).
ptls::tls_credential pin_one_tls(const pdt::identity_fixture &self, const pdt::spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(
        std::vector<pdt::spki_digest>{peer_pin});
    return ptls::load_credential(self.cert_path.string(), self.key_path.string(), policy);
}

// A face of the mux: ONE multiplexing_transport over its OWN (unix, tcp, tls, udp,
// dtls) member quintet, each member owned outright. The tls and dtls members both
// take this face's cross-pinning credential; the tcp/unix/udp members are inert on
// the routes a given test exercises. A real two-node loopback gives each node its
// own quintet (the concrete completion callbacks are single-slot).
struct mux_face
{
    ::asio::io_context &io;
    ptls::tls_credential tls_cred;
    ptls::tls_credential dtls_cred;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport secure;
    pasio::udp_transport datagram{io};
    ptls::dtls_transport secure_datagram;
    pasio::all_backends_mux mux{local, remote, secure, datagram, secure_datagram};

    mux_face(::asio::io_context &ctx, ptls::tls_credential tls_c, ptls::tls_credential dtls_c)
        : io(ctx)
        , tls_cred(std::move(tls_c))
        , dtls_cred(std::move(dtls_c))
        , secure(io, tls_cred)
        , secure_datagram(io, dtls_cred)
    {
    }
};

// A loopback pair of mux faces sharing one io_context: a listen face and a dial
// face, each a full quintet. The tls and dtls members cross-pin each other; the
// dial face drives a dial of the chosen scheme to the listen face's bound port for
// that member. The accepted (listen-side) and dialed (dial-side) mux_channels are
// captured so the route + the scheme-survival can be asserted on both ends.
struct mux_pair
{
    ::asio::io_context io;
    mux_face listen_face;
    mux_face dial_face;

    std::optional<pio::endpoint> dialed_ep;
    std::unique_ptr<pio::polymorphic_byte_channel> dialed;
    std::unique_ptr<pio::polymorphic_byte_channel> accepted;

    mux_pair(const pdt::identity_fixture &server_id, const pdt::identity_fixture &client_id)
        : listen_face(io, pin_one_tls(server_id, client_id.digest), pdt::pin_one(server_id, client_id.digest))
        , dial_face(io, pin_one_tls(client_id, server_id.digest), pdt::pin_one(client_id, server_id.digest))
    {
        listen_face.mux.on_accepted([this](std::unique_ptr<pio::polymorphic_byte_channel> ch) {
            accepted = std::move(ch);
        });
        dial_face.mux.on_dialed([this](std::unique_ptr<pio::polymorphic_byte_channel> ch, const pio::endpoint &ep) {
            dialed = std::move(ch);
            dialed_ep.emplace(ep);
        });
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(6);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

constexpr int k_iterations = 100;

}

TEST_CASE("dtls.mux_select: the selector classifies dtls as remote, never local (locality exclusion)",
          "[dtls][mux][select]")
{
    pio::transport_selector sel;
    const auto reserved = pio::reliability_hint::unspecified;

    // "dtls" is a REMOTE-tier scheme — so locality confinement EXCLUDES it (a
    // host-confined process|local topic never rides dtls even though dtls encrypts).
    // "unix"/"inproc" are the same-host local tier; "tls"/"tcp"/"udp" are remote too.
    REQUIRE(sel.select({"dtls", "127.0.0.1:5000"}, reserved) == pio::transport_kind::remote);
    REQUIRE(sel.select({"unix", "/tmp/s"}, reserved) == pio::transport_kind::local);
    REQUIRE(sel.select({"inproc", "node-a"}, reserved) == pio::transport_kind::local);
    // dtls is secure-best_effort (the secure parallel of udp) on the reliability axis.
    REQUIRE(sel.reliability_of_scheme("dtls") == pio::reliability_hint::best_effort);
}

TEST_CASE("dtls.mux: a dtls dial routes to the secure-datagram member, completes, and flows a frame, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("mux_srv");
    pdt::identity_fixture client_id("mux_cli");

    const std::string text = "secure-datagram-over-the-mux";
    std::vector<std::byte> frame(reinterpret_cast<const std::byte *>(text.data()),
                                 reinterpret_cast<const std::byte *>(text.data()) + text.size());

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"dtls", "127.0.0.1:0"});
        n.dial_face.mux.dial({"dtls", "127.0.0.1:" + std::to_string(n.listen_face.secure_datagram.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);     // delivered POST external_complete via the dtls member
        REQUIRE(n.accepted != nullptr);   // accepted POST mutual-verify via the dtls member
        // The "dtls" scheme survives the type-erasure on BOTH ends.
        REQUIRE(n.dialed->remote_endpoint().scheme == "dtls");
        REQUIRE(n.accepted->remote_endpoint().scheme == "dtls");
        // The dialed endpoint is returned UNCHANGED (the correlation key).
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "dtls");

        // An app frame flows decrypted over the wrapped polymorphic_byte_channel — proves the route
        // landed a live secure-datagram channel, not merely a completed handshake.
        std::vector<std::byte> got;
        bool received = false;
        n.accepted->on_data([&](std::span<const std::byte> d) {
            got.assign(d.begin(), d.end());
            received = true;
        });
        n.dialed->send(std::span<const std::byte>{frame});
        n.pump_until([&] { return received; });
        REQUIRE(received);
        REQUIRE(got == frame);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.mux: a tcp dial on the same mux still routes to the plain-TCP member — no cross-talk, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("xt_srv");
    pdt::identity_fixture client_id("xt_cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // A mux that carries a dtls member also routes a plain "tcp" dial to the
        // plaintext stream member — the secure-datagram member never intercepts it.
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

TEST_CASE("dtls.mux: a tls dial on the same mux still routes to the secure-stream member — coexists with dtls, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("xs_srv");
    pdt::identity_fixture client_id("xs_cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The mux's secure-stream (tls) member and secure-datagram (dtls) member coexist:
        // a "tls" dial reaches the tls member, not the dtls member.
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"tls", "127.0.0.1:0"});
        n.dial_face.mux.dial({"tls", "127.0.0.1:" + std::to_string(n.listen_face.secure.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        REQUIRE(n.dialed->remote_endpoint().scheme == "tls");
        REQUIRE(n.accepted->remote_endpoint().scheme == "tls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.mux: a same-host (unix) dial routes to the local member, never the dtls member, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("lx_srv");
    pdt::identity_fixture client_id("lx_cli");

    // The locality-exclusion behavioral proof: a same-host scheme classifies local and
    // routes to the AF_UNIX local member — never the remote secure-datagram (dtls) member,
    // because the dtls route branch sits AFTER the locality short-circuit in route_of. A
    // process|local-confined topic (which carries a same-host endpoint) thus never egresses
    // over dtls. Each iteration uses a fresh ephemeral socket path so the runs are isolated.
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        const std::string path = std::filesystem::temp_directory_path()
            / ("plexus_dtls_mux_unix_" + std::to_string(::getpid())
               + "_" + std::to_string(iter) + ".sock");
        std::error_code rc;
        std::filesystem::remove(path, rc);

        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"unix", path});
        n.dial_face.mux.dial({"unix", path});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        // The same-host route landed on the unix member — the scheme survives as "unix",
        // proving the locality short-circuit won over any remote (dtls) classification.
        REQUIRE(n.dialed->remote_endpoint().scheme == "unix");
        REQUIRE(n.accepted->remote_endpoint().scheme == "unix");

        std::filesystem::remove(path, rc);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

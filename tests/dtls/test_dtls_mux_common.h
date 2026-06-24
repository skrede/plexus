#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_MUX_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_MUX_COMMON_H

#include "dtls_test_support.h"

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

namespace pdt   = plexus::dtls_test;
namespace pasio = plexus::asio;
namespace ptls  = plexus::tls;
namespace pio   = plexus::io;

static_assert(plexus::io::transport_backend<pasio::all_backends_mux, plexus::muxify<pasio::asio_policy>>);

namespace dtls_mux_fixture {

// Mint a TLS (TLS-over-TCP) credential for `self` pinning exactly `peer_pin` — the
// secure stream member needs a TLS_method SSL_CTX (NOT the DTLS one pin_one mints).
inline ptls::tls_credential pin_one_tls(const pdt::identity_fixture &self, const pdt::spki_digest &peer_pin)
{
    auto policy = std::make_shared<const pio::security::spki_pin_policy>(std::vector<pdt::spki_digest>{peer_pin});
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
        listen_face.mux.on_accepted([this](std::unique_ptr<pio::polymorphic_byte_channel> ch) { accepted = std::move(ch); });
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
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(6);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

constexpr int k_iterations = 100;

}

#endif

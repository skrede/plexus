#include "plexus/asio/mux_policy.h"
#include "plexus/asio/mux_channel.h"
#include "plexus/asio/mux_selector.h"
#include "plexus/asio/mux_transport.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/tls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace ptls = plexus::tls;
namespace pio = plexus::io;

static_assert(plexus::io::transport_backend<pasio::multiplexing_transport, pasio::mux_policy>);

namespace {

// Stands up a multiplexing transport over a real AF_UNIX (local) + TCP (remote) pair,
// listening on the remote family at an ephemeral loopback port, dials a remote (scheme
// "tcp") endpoint, and captures BOTH ends: the dialed mux_channel (the client end, via
// on_dialed) and the accepted mux_channel (the server end, via on_accepted). Each
// instance owns a fresh io_context + transports so looped iterations are independent.
struct remote_dial_link
{
    ::asio::io_context io;
    // The secure member is never exercised on this tcp-only route: its credential
    // stays a default (invalid) one — the SSL_CTX is only ever touched when a "tls"
    // channel is actually dialed/accepted, which this link never does.
    ptls::tls_credential no_tls;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport secure{io, no_tls};
    // The datagram member stays inert on this tcp-only route — its socket is only ever
    // bound when a "udp" channel is actually dialed/accepted, which this link never does.
    pasio::udp_transport datagram{io};
    pasio::multiplexing_transport mux{local, remote, secure, datagram};

    std::optional<pio::endpoint> dialed_ep;
    std::unique_ptr<pasio::mux_channel> dialed;
    std::unique_ptr<pasio::mux_channel> accepted;

    remote_dial_link()
    {
        mux.on_dialed([this](std::unique_ptr<pasio::mux_channel> ch, const pio::endpoint &ep) {
            dialed = std::move(ch);
            dialed_ep.emplace(ep);
        });
        mux.on_accepted([this](std::unique_ptr<pasio::mux_channel> ch) {
            accepted = std::move(ch);
        });

        mux.listen({"tcp", "127.0.0.1:0"});
        mux.dial({"tcp", "127.0.0.1:" + std::to_string(remote.port())});
    }

    template <typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

}

TEST_CASE("mux select: a remote endpoint dials over TCP through the erased channel, looped",
          "[integration][mux][select][asio]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        remote_dial_link l;
        l.pump_until([&] { return l.dialed && l.accepted; });

        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);
        // The selector picked the remote (TCP) transport: the dialed channel reports the
        // "tcp" scheme through the erasure.
        REQUIRE(l.dialed->remote_endpoint().scheme == "tcp");
        // The mux passed the dialed endpoint through unchanged (registry correlation key).
        REQUIRE(l.dialed_ep.has_value());
        REQUIRE(l.dialed_ep->scheme == "tcp");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("mux select: an ACCEPTED TCP connection carries the tcp scheme through the erasure, looped",
          "[integration][mux][select][asio]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        remote_dial_link l;
        l.pump_until([&] { return l.dialed && l.accepted; });

        REQUIRE(l.accepted != nullptr);
        // The accepted (inbound, server-side) channel's scheme is inherited from the
        // concrete backend that accepted it — load-bearing for attach-time tier
        // classification of server-side subscribers; it must NOT be lost in the erasure.
        REQUIRE(l.accepted->remote_endpoint().scheme == "tcp");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

#include "plexus/asio/all_backends_mux.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <unistd.h>

#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;
namespace ptls = plexus::tls;
namespace pio = plexus::io;

static_assert(plexus::io::transport_backend<pasio::all_backends_mux, plexus::muxify<pasio::asio_policy>>);

namespace {

// A per-instance owner-only temp directory + a SHORT socket path within it (well under
// the sun_path cap). The directory is removed on teardown after the socket file is
// unlinked (the listener unlinks it on stop, but rmdir needs an empty dir).
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char tmpl[] = "/tmp/pxm-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir = made ? made : "";
        path = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// Stands up a multiplexing transport over a real AF_UNIX (local) + TCP (remote) pair,
// listening on the local family, dials a same-host (scheme "unix") endpoint, and
// captures BOTH ends of the resulting connection: the dialed polymorphic_byte_channel (the client
// end, via on_dialed) and the accepted polymorphic_byte_channel (the server end, via on_accepted).
// Each instance owns a fresh io_context + transports + socket path so looped iterations
// are independent.
struct local_dial_link
{
    temp_sock sock;
    ::asio::io_context io;
    // The secure member is never exercised on this same-host route: its credential
    // stays a default (invalid) one — the SSL_CTX is only ever touched when a "tls"
    // channel is actually dialed/accepted, which this link never does.
    ptls::tls_credential no_tls;
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};
    ptls::tls_transport secure{io, no_tls};
    // The datagram member stays inert on this same-host route — its socket is only ever
    // bound when a "udp" channel is actually dialed/accepted, which this link never does.
    pasio::udp_transport datagram{io};
    // The secure-datagram (DTLS) member is likewise inert here: it reuses the same default
    // (invalid) credential and binds no socket unless a "dtls" channel is dialed/accepted.
    ptls::dtls_transport secure_datagram{io, no_tls};
    pasio::all_backends_mux mux{local, remote, secure, datagram, secure_datagram};

    std::optional<pio::endpoint> dialed_ep;
    std::unique_ptr<pio::polymorphic_byte_channel> dialed;
    std::unique_ptr<pio::polymorphic_byte_channel> accepted;

    local_dial_link()
    {
        mux.on_dialed([this](std::unique_ptr<pio::polymorphic_byte_channel> ch, const pio::endpoint &ep) {
            dialed = std::move(ch);
            dialed_ep.emplace(ep);
        });
        mux.on_accepted([this](std::unique_ptr<pio::polymorphic_byte_channel> ch) {
            accepted = std::move(ch);
        });

        mux.listen({"unix", sock.path});
        mux.dial({"unix", sock.path});
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

TEST_CASE("mux select: a same-host endpoint dials over AF_UNIX through the erased channel, looped",
          "[integration][mux][select][unix]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        local_dial_link l;
        l.pump_until([&] { return l.dialed && l.accepted; });

        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);
        // The selector picked the local (AF_UNIX) transport: the dialed channel reports
        // the "unix" scheme through the erasure.
        REQUIRE(l.dialed->remote_endpoint().scheme == "unix");
        // The mux passed the dialed endpoint through unchanged (registry correlation key).
        REQUIRE(l.dialed_ep.has_value());
        REQUIRE(l.dialed_ep->scheme == "unix");
        REQUIRE(l.dialed_ep->address == l.sock.path);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("mux select: an ACCEPTED AF_UNIX connection carries the unix scheme through the erasure, looped",
          "[integration][mux][select][unix]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        local_dial_link l;
        l.pump_until([&] { return l.dialed && l.accepted; });

        REQUIRE(l.accepted != nullptr);
        // The accepted (inbound, server-side) channel's scheme is inherited from the
        // concrete backend that accepted it — load-bearing for attach-time tier
        // classification of server-side subscribers; it must NOT be lost in the erasure.
        REQUIRE(l.accepted->remote_endpoint().scheme == "unix");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

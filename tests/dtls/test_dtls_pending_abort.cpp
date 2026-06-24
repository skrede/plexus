#include "dtls_test_support.h"

#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/dtls_channel.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <memory>
#include <string>
#include <chrono>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;

// The asan dial-abort leak proof for the transport-owned pending dials: a dtls dial
// whose handshake never completes (the peer never answers) is torn down WITH THE
// HANDSHAKE STILL PENDING by destroying the io_context + the transports. The
// transport owns the in-flight channel in m_pending, so its destructor frees every
// pending channel — no leak, no use-after-free (the structural fix vs the TLS
// self-owning-channel cycle, which leaked the channel on mid-handshake teardown).
// Under asan this case is the proof; without asan it still exercises the teardown
// path. The retransmit timer (armed during the pending handshake) must also be
// canceled cleanly by the channel destructor (no timer-callback UAF after free).

TEST_CASE("dtls.pending_abort: a transport destroyed mid-handshake frees the pending dial, looped", "[dtls][pending_abort]")
{
    pdt::identity_fixture srv("pa_srv");
    pdt::identity_fixture cli("pa_cli");

    // Dial a dead port (nothing listens), so the ClientHello goes out, no response
    // ever arrives, and the dial stays pending with the retransmit timer armed. Tear
    // everything down inside the loop body — the transport-owned m_pending must free
    // the channel and the io_context destruction must not fire a dangling timer.
    constexpr int k_iterations = 100;
    int aborted                = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        ::asio::io_context io;
        auto client_cred = pdt::pin_one(cli, srv.digest);
        ptls::dtls_transport client(io, client_cred);

        bool dialed = false;
        bool failed = false;
        client.on_dialed([&](std::unique_ptr<ptls::dtls_channel>, const pdt::pio::endpoint &) { dialed = true; });
        client.on_dial_failed([&](const pdt::pio::endpoint &, pdt::pio::io_error) { failed = true; });

        // 127.0.0.1:1 — a port nothing listens on; the ClientHello is sent, no peer
        // ever answers, the dial stays pending with the retransmit timer armed.
        client.dial({"dtls", "127.0.0.1:1"});

        // Pump a short window so the ClientHello is in flight + the retransmit timer is
        // armed, then let io + client go out of scope here WITHOUT completing.
        pdt::settle(io, std::chrono::milliseconds{30});
        REQUIRE_FALSE(dialed); // never completes (no peer)
        ++aborted;
        (void)failed; // a retransmit-exhaustion fail is acceptable; a leak is not
    }
    REQUIRE(aborted == k_iterations);
}

TEST_CASE("dtls.pending_abort: a silent listening server leaves the dial pending until teardown, "
          "looped",
          "[dtls][pending_abort]")
{
    pdt::identity_fixture srv("ps_srv");
    pdt::identity_fixture cli("ps_cli");

    // A relay that BLACK-HOLES every client datagram: the server is real and bound,
    // but the relay drops the ClientHello, so the server never sees it and the dial
    // stays pending. Tearing down (io, transports, relay) mid-handshake frees the
    // transport-owned pending channel cleanly — the exact leak the TLS self-owning
    // cycle had, now closed by transport-owned dials. Run under asan to catch a leak.
    constexpr int k_iterations = 100;
    int aborted                = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        ::asio::io_context io;
        auto server_cred = pdt::pin_one(srv, cli.digest);
        auto client_cred = pdt::pin_one(cli, srv.digest);
        ptls::dtls_transport server(io, server_cred);
        ptls::dtls_transport client(io, client_cred);

        bool dialed = false;
        client.on_dialed([&](std::unique_ptr<ptls::dtls_channel>, const pdt::pio::endpoint &) { dialed = true; });

        server.listen({"dtls", "127.0.0.1:0"});
        auto link = std::make_unique<pdt::relay>(io, server.port());
        for(int k = 0; k < 16; ++k)
            link->script.push_back(pdt::action::drop); // black-hole every client flight
        client.dial({"dtls", "127.0.0.1:" + std::to_string(link->port())});

        pdt::settle(io, std::chrono::milliseconds{30});
        REQUIRE_FALSE(dialed);
        ++aborted;
    }
    REQUIRE(aborted == k_iterations);
}

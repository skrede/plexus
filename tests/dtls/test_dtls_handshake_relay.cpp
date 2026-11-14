#include "dtls_relay_link.h"
#include "test_dtls_handshake_common.h"

using pdt::relay_link;

TEST_CASE("dtls.verify: a cross-pinned pair accepts; mis-pin / unpinned / empty fail closed, looped", "[dtls][verify]")
{
    pdt::identity_fixture srv("v_srv");
    pdt::identity_fixture cli("v_cli");
    pdt::identity_fixture other("v_other"); // a 3rd identity for the wrong-digest pin

    constexpr int k_iterations = 100;

    SECTION("cross-pinned: accepts and completes")
    {
        int ok = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
            plexus::testing::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; });
            REQUIRE(l.dialed != nullptr);
            REQUIRE(l.accepted != nullptr);
            ++ok;
        }
        REQUIRE(ok == k_iterations);
    }

    SECTION("server pins the WRONG client digest: fail closed (no channel)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, other.digest), pdt::pin_one(cli, srv.digest));
            plexus::testing::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); }, std::chrono::milliseconds{1500});
            REQUIRE(l.accepted == nullptr);
            REQUIRE(l.dialed == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }

    SECTION("client pins the WRONG server digest: fail closed (no channel)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, other.digest));
            plexus::testing::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); }, std::chrono::milliseconds{1500});
            REQUIRE(l.dialed == nullptr);
            REQUIRE(l.accepted == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }

    SECTION("empty (no-pin) policy on the server: fail closed (accepts nothing)")
    {
        int closed = 0;
        for(int i = 0; i < k_iterations; ++i)
        {
            relay_link l(pdt::pin_none(srv), pdt::pin_one(cli, srv.digest));
            plexus::testing::pump_until(l.io, [&] { return l.dial_failed || (l.accepted && l.dialed); }, std::chrono::milliseconds{1500});
            REQUIRE(l.accepted == nullptr);
            REQUIRE(l.dialed == nullptr);
            ++closed;
        }
        REQUIRE(closed == k_iterations);
    }
}

TEST_CASE("dtls.dial_abort: an io_context destroyed mid-handshake leaks nothing, looped", "[dtls][dial_abort]")
{
    pdt::identity_fixture srv("ab_srv");
    pdt::identity_fixture cli("ab_cli");

    // Dial into a relay that DROPS every client datagram, so the handshake never
    // completes — the dial stays pending. Destroying the io_context + transports
    // mid-handshake must free the transport-owned pending channel cleanly (the leak
    // the TLS self-owning-channel cycle had). Run under asan to catch a leak/UAF.
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
        for(int k = 0; k < 8; ++k)
            link->script.push_back(pdt::action::drop); // black-hole the client flights
        client.dial({"dtls", "127.0.0.1:" + std::to_string(link->port())});

        // Pump a short window so the ClientHello is in flight, then tear everything
        // down WITHOUT completing (io, transports, relay all go out of scope here).
        pdt::settle(io, std::chrono::milliseconds{30});
        REQUIRE_FALSE(dialed);
        ++aborted;
    }
    REQUIRE(aborted == k_iterations);
}

TEST_CASE("dtls.handshake_loss: the handshake completes through a lossy relay, looped", "[dtls][handshake_loss]")
{
    pdt::identity_fixture srv("loss_srv");
    pdt::identity_fixture cli("loss_cli");

    constexpr int k_iterations = 100;
    int completed              = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        relay_link l(pdt::pin_one(srv, cli.digest), pdt::pin_one(cli, srv.digest));
        // Drop the first ClientHello, duplicate the cookie'd retry, then pass — OpenSSL
        // retransmit (NOT the ARQ) must recover. Subsequent flights pass.
        l.link->script = {pdt::action::drop, pdt::action::duplicate};
        plexus::testing::pump_until(l.io, [&] { return (l.accepted && l.dialed) || l.dial_failed; });
        REQUIRE_FALSE(l.dial_failed);
        REQUIRE(l.dialed != nullptr);
        REQUIRE(l.accepted != nullptr);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

#include "dtls_pumps.h"
#include "dtls_silent_peer.h"
#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <chrono>
#include <memory>
#include <vector>
#include <cstdint>
#include <utility>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace std::chrono_literals;

TEST_CASE("dtls.handshake_timeout: a dial whose peer never answers fails with timed_out once its retransmits are spent", "[dtls][retransmit]")
{
    pdt::identity_fixture id("ht_dial");
    ::asio::io_context io;
    const ptls::tls_credential cred = pdt::pin_one(id, id.digest);
    pdt::silent_socket peer(io);
    ptls::dtls_transport client = pdt::transport_with_ceiling(io, cred, 20ms);
    std::vector<std::pair<pio::endpoint, pio::io_error>> failures;
    std::int32_t dialed = 0;
    client.on_dialed([&dialed](std::unique_ptr<ptls::dtls_channel>, const pio::endpoint &) { ++dialed; });
    client.on_dial_failed([&failures](const pio::endpoint &ep, pio::io_error e) { failures.emplace_back(ep, e); });

    client.dial(peer.endpoint());
    plexus::testing::pump_until(io, [&failures] { return !failures.empty(); }, 2s);
    pdt::settle(io);
    const std::int32_t received = peer.take_received();

    CAPTURE(received);
    REQUIRE(failures.size() == 1);
    CHECK(failures.front().first == peer.endpoint());
    CHECK(failures.front().second == pio::io_error::timed_out);
    CHECK(dialed == 0);
    CHECK(received >= 2);
}

TEST_CASE("dtls.handshake_timeout: an accepted half-open whose client falls silent after the cookie exchange is retired once its retransmits are spent", "[dtls][retransmit]")
{
    pdt::identity_fixture srv("ht_srv");
    pdt::identity_fixture cli("ht_cli");
    ::asio::io_context io;
    const ptls::tls_credential cred = pdt::pin_one(srv, cli.digest);
    ptls::dtls_transport server     = pdt::transport_with_ceiling(io, cred, 20ms);
    std::int32_t accepted           = 0;
    server.on_accepted([&accepted](std::unique_ptr<ptls::dtls_channel>) { ++accepted; });
    server.listen({"dtls", "127.0.0.1:0"});
    pdt::cookie_client client(io, cli, srv, server.port());

    client.exchange_cookie();
    pdt::settle(io, 400ms);
    client.take();
    client.probe();

    CHECK(client.await_handshake(pdt::k_hello_verify_request, 2s));
    CHECK(accepted == 0);
}

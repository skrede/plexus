#include "dtls_pumps.h"
#include "dtls_silent_peer.h"
#include "dtls_identity_fixture.h"

#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/tls/detail/dtls_retransmit.h"

#include "plexus/asio/udp_server.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/fragmentation.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <cstdint>
#include <stdexcept>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace std::chrono_literals;

namespace {

struct channel_parts
{
    ::asio::io_context io;
    ptls::tls_credential cred;
    pio::security::cookie_secret cookie;
    plexus::asio::udp_server sock;

    explicit channel_parts(const pdt::identity_fixture &id)
            : io()
            , cred(pdt::pin_one(id, id.digest))
            , cookie(ptls::make_cookie_secret())
            , sock(io)
    {
    }

    void build(std::chrono::microseconds ceiling)
    {
        const ::asio::ip::udp::endpoint dest(::asio::ip::make_address("127.0.0.1"), 9);
        ptls::dtls_channel channel(io, sock, dest, cred, cookie, ptls::dtls_channel::role::client, ptls::dtls_channel::default_max_payload, ptls::dtls_channel::default_record_mtu,
                                   pio::global_default_max_message_bytes, pio::reassembly_memory_budget, ptls::dtls_channel::reassembler_type::config{}.per_message_timeout, ceiling);
    }
};

}

TEST_CASE("dtls.retransmit_ceiling: the schedule starts at one second, doubles and stops at the ceiling", "[dtls][retransmit]")
{
    using ptls::detail::next_retransmit_us;
    CHECK(next_retransmit_us(0, 60s) == 1'000'000u);
    CHECK(next_retransmit_us(1'000'000u, 60s) == 2'000'000u);
    CHECK(next_retransmit_us(32'000'000u, 60s) == 60'000'000u);
    CHECK(next_retransmit_us(60'000'000u, 60s) == 60'000'000u);
    CHECK(next_retransmit_us(0, 20ms) == 20'000u);
    CHECK(next_retransmit_us(20'000u, 20ms) == 20'000u);
    CHECK(next_retransmit_us(0, 300ms) == 300'000u);
    CHECK(next_retransmit_us(3'000'000'000u, 2h) == 4'294'967'295u);
}

TEST_CASE("dtls.retransmit_ceiling: a ceiling under OpenSSL's expiry floor is refused when a transport or channel is built", "[dtls][retransmit]")
{
    pdt::identity_fixture id("rc_refuse");
    channel_parts parts(id);
    const std::array<std::chrono::microseconds, 4> refused{0us, -1ms, 1us, ptls::dtls_channel::min_retransmit_ceiling - 1us};
    for(const std::chrono::microseconds ceiling : refused)
    {
        CHECK_THROWS_AS(pdt::transport_with_ceiling(parts.io, parts.cred, ceiling), std::runtime_error);
        CHECK_THROWS_AS(parts.build(ceiling), std::runtime_error);
    }
    CHECK(ptls::dtls_channel::min_retransmit_ceiling == 15ms);
    CHECK_NOTHROW(pdt::transport_with_ceiling(parts.io, parts.cred, ptls::dtls_channel::min_retransmit_ceiling));
    CHECK_NOTHROW(parts.build(ptls::dtls_channel::min_retransmit_ceiling));
}

TEST_CASE("dtls.retransmit_ceiling: a dial to a silent peer retransmits its ClientHello at the transport's ceiling", "[dtls][retransmit]")
{
    pdt::identity_fixture id("rc_dial");
    ::asio::io_context io;
    const ptls::tls_credential cred = pdt::pin_one(id, id.digest);
    pdt::silent_socket peer(io);
    ptls::dtls_transport client = pdt::transport_with_ceiling(io, cred, 20ms);
    std::int32_t dialed         = 0;
    client.on_dialed([&dialed](std::unique_ptr<ptls::dtls_channel>, const pio::endpoint &) { ++dialed; });

    client.dial(peer.endpoint());
    pdt::settle(io, 300ms);

    CHECK(peer.take_received() >= 4);
    CHECK(dialed == 0);
}

TEST_CASE("dtls.retransmit_ceiling: a default transport sends no retransmit within OpenSSL's first second", "[dtls][retransmit]")
{
    pdt::identity_fixture id("rc_default");
    ::asio::io_context io;
    const ptls::tls_credential cred = pdt::pin_one(id, id.digest);
    pdt::silent_socket peer(io);
    ptls::dtls_transport client(io, cred);

    client.dial(peer.endpoint());
    pdt::settle(io, 300ms);

    CHECK(peer.take_received() == 1);
}

TEST_CASE("dtls.retransmit_ceiling: an accepted handshake whose client falls silent retransmits its flight at the transport's ceiling", "[dtls][retransmit]")
{
    pdt::identity_fixture srv("rc_srv");
    pdt::identity_fixture cli("rc_cli");
    ::asio::io_context io;
    const ptls::tls_credential cred = pdt::pin_one(srv, cli.digest);
    ptls::dtls_transport server     = pdt::transport_with_ceiling(io, cred, 20ms);
    std::int32_t accepted           = 0;
    server.on_accepted([&accepted](std::unique_ptr<ptls::dtls_channel>) { ++accepted; });
    server.listen({"dtls", "127.0.0.1:0"});
    pdt::cookie_client client(io, cli, srv, server.port());

    client.exchange_cookie();

    CHECK(client.count_led_by(pdt::k_server_hello, 300ms) >= 3);
    CHECK(accepted == 0);
}

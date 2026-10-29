#include "dtls_raw_client.h"
#include "dtls_identity_fixture.h"

#include "asio/queued_teardown.h"

#include "support/retire_drain.h"

#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/tls_credential.h"

#include "plexus/asio/udp_server.h"

#include "plexus/io/detail/channel_graveyard.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace pdt  = plexus::dtls_test;
namespace ptls = plexus::tls;
namespace pio  = plexus::io;

using namespace std::chrono_literals;
using plexus::testing::queued_action;
using plexus::testing::retire_and_drain;
using plexus::testing::k_teardown_iterations;

namespace {

const std::array<std::byte, 16> k_payload{};

// A dialing channel whose peer is a bound socket that never answers, so its handshake only ever retransmits.
struct silent_dial
{
    ::asio::io_context io;
    ptls::tls_credential cred;
    pio::security::cookie_secret cookie;
    plexus::asio::udp_server sock;
    ::asio::ip::udp::socket peer;
    std::unique_ptr<ptls::dtls_channel> channel;

    explicit silent_dial(const pdt::identity_fixture &id)
            : io()
            , cred(pdt::pin_one(id, id.digest))
            , cookie(ptls::make_cookie_secret())
            , sock(io)
            , peer(io, ::asio::ip::udp::endpoint(::asio::ip::make_address("127.0.0.1"), 0))
            , channel()
    {
        sock.start(::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0));
        channel = std::make_unique<ptls::dtls_channel>(io, sock, peer.local_endpoint(), cred, cookie, ptls::dtls_channel::role::client);
        channel->start_handshake();
    }

    std::int32_t take_received()
    {
        std::int32_t taken = 0;
        std::array<std::byte, 2048> buf{};
        ::asio::ip::udp::endpoint from;
        std::error_code ec;
        while(peer.available(ec) != 0 && !ec)
        {
            peer.receive_from(::asio::buffer(buf), from, 0, ec);
            ++taken;
        }
        return taken;
    }
};

}

TEST_CASE("dtls_retire_drain a delivery queued before close is not handed to the consumer", "[dtls][teardown]")
{
    pdt::identity_fixture server_id("rd_srv");
    pdt::identity_fixture client_id("rd_cli");
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        pdt::raw_client h(server_id, client_id);
        h.handshake();
        std::int32_t delivered = 0;
        h.channel->on_data([&delivered](std::span<const std::byte>) { ++delivered; });
        h.send_whole(k_payload);

        const auto outcome = retire_and_drain(h.channel, [&] { h.pump(); });

        REQUIRE(outcome.erased == 1);
        CHECK(delivered == 0);
    }
}

TEST_CASE("dtls_retire_drain a channel closed by the peer's close_notify is still erased when retired", "[dtls][teardown]")
{
    pdt::identity_fixture server_id("rd_srv");
    pdt::identity_fixture client_id("rd_cli");
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        pdt::raw_client h(server_id, client_id);
        h.handshake();
        h.send_close_notify();
        h.pump();
        REQUIRE_FALSE(h.channel->is_open());

        const auto outcome = retire_and_drain(h.channel, [&] { h.pump(); });

        CHECK(outcome.erased == 1);
        CHECK(outcome.empty);
    }
}

// The retire runs in the reactor pass that already queued the handshake's first retransmit expiry.
TEST_CASE("dtls_retire_drain a retransmit expiry queued in the closing pass does not drive the engine after close", "[dtls][teardown]")
{
    pdt::identity_fixture id("rd_dial");
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        silent_dial h(id);
        plexus::testing::pump_until(h.io, [&] { return h.peer.available() != 0; }, 5s);

        std::int32_t erased = 0;
        pio::detail::channel_graveyard<ptls::dtls_channel, 1> graveyard;
        queued_action closing{h.io, [&]
                              {
                                  h.take_received();
                                  graveyard.retire(std::move(h.channel), [&erased] { ++erased; });
                              }};
        closing.run_after(1200ms);

        REQUIRE(erased == 1);
        CHECK(h.take_received() == 0);
    }
}

// A delivery queued, the handshake's retransmit wait still armed and a partial message held: the erase must be the
// last handler the drain runs.
TEST_CASE("dtls_retire_drain a retired channel's posts and timers all complete before its on_closed", "[dtls][teardown]")
{
    pdt::identity_fixture server_id("rd_srv");
    pdt::identity_fixture client_id("rd_cli");
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        pdt::raw_client h(server_id, client_id);
        h.handshake();
        std::int32_t delivered = 0;
        h.channel->on_data([&delivered](std::span<const std::byte>) { ++delivered; });
        h.send_first_of_two(k_payload);
        h.send_whole(k_payload);

        std::int32_t ran          = 0;
        std::int32_t ran_at_erase = -1;
        const auto drain_counting = [&]
        {
            h.io.restart();
            while(h.io.poll_one() != 0)
                ++ran;
            while(h.io.run_one_for(20ms) != 0)
                ++ran;
        };
        const auto outcome = retire_and_drain(h.channel, drain_counting, [&] { ran_at_erase = ran; });

        REQUIRE(outcome.erased == 1);
        REQUIRE(outcome.empty);
        REQUIRE(delivered == 0);
        REQUIRE(ran_at_erase >= 3);
        REQUIRE(ran == ran_at_erase + 1);
    }
}

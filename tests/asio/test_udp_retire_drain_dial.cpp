#include "queued_teardown.h"

#include "support/retire_drain.h"

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/endpoint.h"

#include "plexus/datagram/detail/udp_handshake_frame.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/ip/udp.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace pasio    = plexus::asio;
namespace pio      = plexus::io;
namespace datagram = plexus::datagram::detail;

using namespace std::chrono_literals;
using plexus::testing::retire_and_drain;
using plexus::testing::k_teardown_iterations;

namespace {

constexpr std::chrono::milliseconds k_first_rung{100};

// A dialing transport whose peer is a bare socket the test answers by hand, so the paired response can be made to
// arrive in the reactor pass that also queues the handshake's first retransmit expiry.
struct hand_answered_dial
{
    ::asio::io_context io;
    ::asio::ip::udp::socket peer;
    pasio::udp_transport client;
    std::unique_ptr<pasio::udp_channel> dialed;

    hand_answered_dial()
            : io()
            , peer(io, ::asio::ip::udp::endpoint{::asio::ip::make_address("127.0.0.1"), 0})
            , client(io, pasio::udp_channel::default_max_payload, {k_first_rung, 4 * k_first_rung, 8 * k_first_rung})
            , dialed()
    {
        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
    }

    void answer_request()
    {
        std::array<std::byte, 64> buf{};
        ::asio::ip::udp::endpoint from;
        const std::size_t n = peer.receive_from(::asio::buffer(buf), from);
        const auto request  = datagram::decode_handshake(std::span<const std::byte>{buf.data(), n});
        REQUIRE(request);
        std::vector<std::byte> response;
        datagram::encode_handshake_into(response, datagram::udp_hs_type::response, request->mode, request->initial_seq);
        peer.send_to(::asio::buffer(response), from);
    }

    void drain()
    {
        for(std::int32_t i = 0; i < 256; ++i)
        {
            io.poll();
            if(io.stopped())
                io.restart();
        }
    }
};

}

// The response is in flight before the first rung expires, so one reactor pass hands over the response and then the
// expiry, and the dial resolves with that expiry already queued.
TEST_CASE("udp_retire_drain a handshake expiry queued when the dial resolves touches nothing", "[udp][teardown]")
{
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        hand_answered_dial h;
        const auto first_expiry = std::chrono::steady_clock::now() + k_first_rung;
        h.client.dial({"udp", "127.0.0.1:" + std::to_string(h.peer.local_endpoint().port())});
        plexus::testing::pump_until(h.io, [&] { return h.peer.available() != 0; }, 5s);
        h.answer_request();
        std::this_thread::sleep_until(first_expiry + 20ms);
        h.io.poll();
        REQUIRE(h.dialed != nullptr);

        const auto outcome = retire_and_drain(h.dialed, [&] { h.drain(); });

        CHECK(outcome.erased == 1);
        CHECK(h.peer.available() == 0);
    }
}

#include "udp_loopback.h"
#include "queued_teardown.h"

#include "support/retire_drain.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_channel.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/channel_graveyard.h"

#include "plexus/datagram/detail/udp_reliable_arq.h"
#include "plexus/datagram/detail/udp_handshake_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/ip/udp.hpp>
#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace pasio    = plexus::asio;
namespace pio      = plexus::io;
namespace datagram = plexus::datagram::detail;

using namespace std::chrono_literals;
using plexus::testing::loopback;
using plexus::testing::queued_action;
using plexus::testing::retire_and_drain;
using plexus::testing::k_teardown_iterations;

namespace {

const ::asio::ip::udp::endpoint k_any_loopback{::asio::ip::make_address("127.0.0.1"), 0};
const std::array<std::byte, 16> k_payload{};

// A channel over its own server whose peer is a bound socket that never answers, so nothing ever acks it.
struct silent_peer
{
    ::asio::io_context io;
    pasio::udp_server server;
    ::asio::ip::udp::socket peer;
    std::unique_ptr<pasio::udp_channel> channel;

    silent_peer(datagram::udp_channel_mode mode, datagram::udp_arq_config arq, std::chrono::milliseconds reassembly_timeout)
            : io()
            , server(io)
            , peer(io, k_any_loopback)
            , channel()
    {
        server.start(k_any_loopback);
        channel = std::make_unique<pasio::udp_channel>(io, server, peer.local_endpoint(), pasio::udp_channel::default_max_payload, arq, pio::congestion::block,
                                                       pasio::udp_channel::default_backpressure_bytes, mode, std::uint16_t{0}, pio::global_default_max_message_bytes,
                                                       pio::reassembly_memory_budget, reassembly_timeout);
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

std::vector<std::byte> best_effort_datagram()
{
    return plexus::wire::wrap_udp(plexus::wire::udp_envelope_kind::best_effort, 0, k_payload);
}

}

TEST_CASE("udp_retire_drain a delivery queued before close is not handed to the consumer", "[udp][teardown]")
{
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        loopback h;
        std::int32_t delivered = 0;
        h.accepted->on_data([&delivered](std::span<const std::byte>) { ++delivered; });
        h.accepted->deliver_inbound(best_effort_datagram());

        const auto outcome = retire_and_drain(h.accepted, [&] { h.drain(); });

        REQUIRE(outcome.erased == 1);
        CHECK(delivered == 0);
    }
}

TEST_CASE("udp_retire_drain a channel whose close notification already ran is still erased when retired", "[udp][teardown]")
{
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        loopback h;
        h.accepted->close();
        h.drain();

        const auto outcome = retire_and_drain(h.accepted, [&] { h.drain(); });

        CHECK(outcome.erased == 1);
        CHECK(outcome.empty);
    }
}

// The retire runs in the reactor pass that already queued the retransmit expiry.
TEST_CASE("udp_retire_drain a reliable channel closed in the pass that queued its retransmit sends nothing after close", "[udp][teardown]")
{
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        silent_peer h{datagram::udp_channel_mode::reliable_datagram, {.initial_rto = 20ms, .min_rto = 1ms}, 1000ms};
        h.channel->send(k_payload);
        plexus::testing::pump_until(h.io, [&] { return h.peer.available() != 0; }, 5s);

        std::int32_t erased = 0;
        pio::detail::channel_graveyard<pasio::udp_channel, 1> graveyard;
        queued_action closing{h.io, [&]
                              {
                                  h.take_received();
                                  graveyard.retire(std::move(h.channel), [&erased] { ++erased; });
                              }};
        closing.run_after(60ms);
        const std::int32_t after_close = h.take_received();

        REQUIRE(erased == 1);
        CHECK(after_close == 0);
    }
}

// The retire runs in the reactor pass that already queued the reassembly timeout.
TEST_CASE("udp_retire_drain a reassembly timeout queued in the closing pass reports no drop after close", "[udp][teardown]")
{
    std::vector<std::byte> first_of_two;
    plexus::wire::wrap_udp_fragment_into(first_of_two, plexus::wire::udp_envelope_kind::best_effort, 0, 1, 0, 2, k_payload);

    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        silent_peer h{datagram::udp_channel_mode::best_effort, {}, 20ms};
        std::int32_t drops = 0;
        h.channel->on_drop([&drops](const pio::detail::drop_event &) { ++drops; });
        h.channel->deliver_inbound(first_of_two);

        std::int32_t erased = 0;
        pio::detail::channel_graveyard<pasio::udp_channel, 1> graveyard;
        queued_action closing{h.io, [&] { graveyard.retire(std::move(h.channel), [&erased] { ++erased; }); }};
        closing.run_after(60ms);

        REQUIRE(erased == 1);
        CHECK(drops == 0);
    }
}

// A reliable channel with a delivery queued and a retransmit wait armed: the erase must be the last handler the drain runs.
TEST_CASE("udp_retire_drain a retired channel's posts and timers all complete before its on_closed", "[udp][teardown]")
{
    for(std::int32_t i = 0; i < k_teardown_iterations; ++i)
    {
        silent_peer h{datagram::udp_channel_mode::reliable_datagram, {.initial_rto = 1000ms}, 1000ms};
        std::int32_t delivered = 0;
        h.channel->on_data([&delivered](std::span<const std::byte>) { ++delivered; });
        h.channel->send(k_payload);
        plexus::testing::pump_until(h.io, [&] { return h.peer.available() != 0; }, 5s);
        h.channel->deliver_inbound(best_effort_datagram());

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
        REQUIRE(ran_at_erase >= 2);
        REQUIRE(ran == ran_at_erase + 1);
    }
}

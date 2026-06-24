// The handshake-ARQ-over-loss leg: a UDP session cannot establish over a lossy link
// without retransmitting the handshake, so this proves the fixed-ladder ARQ
// (250/500/1000ms x3, here a compressed ladder for a fast deterministic run)
// re-sends and STILL establishes when the first handshake datagrams are dropped.
//
// The loss is injected by a deterministic UDP relay fixture (lossy_relay): the client
// dials the relay, the relay forwards datagrams to the real server (and replies back),
// DROPPING the first `drop_count` datagrams it sees in each direction-agnostic stream.
// This relay is the SAME reusable loss-injection seam a later reliable-data-ARQ leg
// composes — a wrapping forwarder, not a one-off.
#pragma once

#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/datagram/detail/udp_handshake_arq.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace udp_handshake_arq_fixture {

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using ms = std::chrono::milliseconds;

// A compressed ladder so the loss legs run fast while exercising the SAME
// retransmit/surrender mechanics as the production 250/500/1000 ladder.
constexpr pasio::udp_transport::arq_type::schedule fast_ladder{ms{20}, ms{40}, ms{80}};

// A deterministic loss-injecting UDP relay between the dialing client and the real
// server. Forwards each datagram both ways but drops the first `drop_count` it sees,
// so the dialer's ARQ must retransmit to get a handshake through. The reusable
// loss-injection fixture (a wrapping forwarder).
struct lossy_relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front; // faces the client
    ::asio::ip::udp::socket back;  // faces the server
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep; // learned from the first client datagram
    ::asio::ip::udp::endpoint from;      // scratch for the active recv
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};
    int drop_count;
    int dropped{0};

    lossy_relay(::asio::io_context &ctx, std::uint16_t server_port, int drops)
            : io(ctx)
            , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
            , drop_count(drops)
    {
        recv_front();
        recv_back();
    }

    std::uint16_t port() const
    {
        return front.local_endpoint().port();
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
                                 [this](std::error_code ec, std::size_t n)
                                 {
                                     if(ec)
                                         return;
                                     client_ep = from;
                                     if(!maybe_drop())
                                         back.send_to(::asio::buffer(front_buf.data(), n), server_ep);
                                     recv_front();
                                 });
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
                                [this](std::error_code ec, std::size_t n)
                                {
                                    if(ec)
                                        return;
                                    if(!maybe_drop() && client_ep.port() != 0)
                                        front.send_to(::asio::buffer(back_buf.data(), n), client_ep);
                                    recv_back();
                                });
    }

    bool maybe_drop()
    {
        if(dropped < drop_count)
        {
            ++dropped;
            return true;
        }
        return false;
    }
};

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{4000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

} // namespace udp_handshake_arq_fixture

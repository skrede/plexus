#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RELAY_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_RELAY_H

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>
#include <asio/buffer.hpp>

#include <span>
#include <array>
#include <deque>
#include <memory>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <system_error>

// The programmable loss-injecting relay (ported from tests/asio/test_udp_reliable_arq.cpp,
// but scripting RAW DTLS handshake-flight datagrams — DTLS records are self-framing, there
// is no ARQ envelope to filter on).
namespace plexus::dtls_test {

// What the relay does to one client->server datagram. The relay always passes
// server->client datagrams (the dialer must hear HelloVerifyRequest / the server
// flights); only the client->server direction is scripted.
enum class action
{
    pass,
    drop,
    duplicate,
    hold
};

// A programmable relay between a dialing client and a real server, forwarding RAW
// DTLS records both ways. For client->server datagrams it consults a scripted
// action queue (drop one, duplicate one, hold-then-release to reorder). Unlike the
// ARQ relay there is no envelope to peek — a DTLS record is opaque self-framed
// bytes, so EVERY client->server datagram is scripted in order.
struct relay
{
    ::asio::io_context &io;
    ::asio::ip::udp::socket front; // faces the client
    ::asio::ip::udp::socket back;  // faces the server
    ::asio::ip::udp::endpoint server_ep;
    ::asio::ip::udp::endpoint client_ep;
    ::asio::ip::udp::endpoint from;
    std::array<std::byte, 2048> front_buf{};
    std::array<std::byte, 2048> back_buf{};

    std::deque<action> script;                // consumed per client->server datagram
    std::vector<std::vector<std::byte>> held; // held datagrams to release out of order
    int seen{0};

    relay(::asio::io_context &ctx, std::uint16_t server_port)
            : io(ctx)
            , front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
    {
        recv_front();
        recv_back();
    }

    std::uint16_t port() const
    {
        return front.local_endpoint().port();
    }

    void send_to_server(std::span<const std::byte> dg)
    {
        auto copy = std::make_shared<std::vector<std::byte>>(dg.begin(), dg.end());
        back.async_send_to(::asio::buffer(*copy), server_ep, [copy](std::error_code, std::size_t) {});
    }

    void recv_front()
    {
        front.async_receive_from(::asio::buffer(front_buf), from,
                                 [this](std::error_code ec, std::size_t n)
                                 {
                                     if(ec)
                                         return;
                                     client_ep = from;
                                     handle_client(std::span<const std::byte>{front_buf.data(), n});
                                     recv_front();
                                 });
    }

    void handle_client(std::span<const std::byte> dg)
    {
        ++seen;
        action a = action::pass;
        if(!script.empty())
        {
            a = script.front();
            script.pop_front();
        }
        switch(a)
        {
            case action::pass:
                send_to_server(dg);
                break;
            case action::drop:
                break; // lost: OpenSSL retransmits
            case action::duplicate:
                send_to_server(dg);
                send_to_server(dg);
                break;
            case action::hold:
                held.emplace_back(dg.begin(), dg.end());
                break;
        }
    }

    // Release every held datagram (out of order, after later ones already passed)
    // to exercise the receiver's reorder path; subsequent retransmits pass normally.
    void release_held()
    {
        for(auto &h : held)
            send_to_server(h);
        held.clear();
    }

    void recv_back()
    {
        back.async_receive_from(::asio::buffer(back_buf), from,
                                [this](std::error_code ec, std::size_t n)
                                {
                                    if(ec)
                                        return;
                                    if(client_ep.port() != 0) // server->client always passes
                                    {
                                        auto copy = std::make_shared<std::vector<std::byte>>(back_buf.data(), back_buf.data() + n);
                                        front.async_send_to(::asio::buffer(*copy), client_ep, [copy](std::error_code, std::size_t) {});
                                    }
                                    recv_back();
                                });
    }
};

}

#endif

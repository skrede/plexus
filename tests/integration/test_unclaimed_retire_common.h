#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_UNCLAIMED_RETIRE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_UNCLAIMED_RETIRE_COMMON_H

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <asio/error.hpp>
#include <asio/buffer.hpp>
#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace unclaimed_retire {

inline constexpr std::array<std::byte, 16> k_bytes{};

template<typename Feed>
void run_window(::asio::io_context &io, Feed feed)
{
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds{300};
    while(std::chrono::steady_clock::now() < until)
    {
        feed();
        io.restart();
        io.run_for(std::chrono::milliseconds{5});
    }
}

inline std::string loopback(std::uint16_t port)
{
    return "127.0.0.1:" + std::to_string(port);
}

// A raw stream socket that keeps writing and notices when the transport's end of it closes.
template<typename Socket>
struct stream_peer
{
    Socket socket;
    bool closed;

    explicit stream_peer(::asio::io_context &io)
            : socket(io)
            , closed(false)
    {
    }

    void feed()
    {
        std::error_code ec;
        socket.non_blocking(true, ec);
        if(!closed)
            (void)socket.write_some(::asio::buffer(k_bytes), ec);
        std::array<std::byte, 256> buf{};
        while(!closed)
        {
            (void)socket.read_some(::asio::buffer(buf), ec);
            if(ec == ::asio::error::would_block)
                return;
            closed = static_cast<bool>(ec);
        }
    }
};

template<typename Socket, typename Endpoint>
bool peer_sees_close(::asio::io_context &io, const Endpoint &target)
{
    stream_peer<Socket> peer{io};
    peer.socket.connect(target);
    run_window(io, [&peer] { peer.feed(); });
    return peer.closed;
}

template<typename Transport>
bool tcp_dial_sees_close(::asio::io_context &io, Transport &transport)
{
    ::asio::ip::tcp::acceptor acceptor{io, ::asio::ip::tcp::endpoint{::asio::ip::make_address("127.0.0.1"), 0}};
    transport.dial({"tcp", loopback(acceptor.local_endpoint().port())});
    stream_peer<::asio::ip::tcp::socket> peer{io};
    acceptor.accept(peer.socket);
    run_window(io, [&peer] { peer.feed(); });
    return peer.closed;
}

// The claimed end a test holds and keeps sending on, so the unclaimed end's peer keeps routing to it.
template<typename Channel>
struct holder
{
    std::unique_ptr<Channel> channel;
    bool closed;

    holder()
            : channel()
            , closed(false)
    {
    }

    void take(std::unique_ptr<Channel> ch)
    {
        channel = std::move(ch);
        channel->on_closed([this] { closed = true; });
    }

    void feed()
    {
        if(channel)
            channel->send(k_bytes);
    }
};

struct inproc_link
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex;
    plexus::inproc::inproc_transport<> listener;
    plexus::inproc::inproc_transport<> dialer;
    std::unique_ptr<plexus::inproc::inproc_channel<>> held;
    bool broken;

    inproc_link()
            : bus()
            , ex(bus)
            , listener(ex, bus)
            , dialer(ex, bus)
            , held()
            , broken(false)
    {
        listener.listen({"inproc", "unclaimed_retire"});
    }

    void take(std::unique_ptr<plexus::inproc::inproc_channel<>> ch)
    {
        held = std::move(ch);
        held->on_error([this](plexus::io::io_error) { broken = true; });
    }
};

template<typename Channel, typename Unclaimed, typename Held>
void run_accept_unclaimed(::asio::io_context &io, Unclaimed &unclaimed, Held &held_side, holder<Channel> &held, const std::string &scheme)
{
    held_side.on_dialed([&held](std::unique_ptr<Channel> ch, const plexus::io::endpoint &) { held.take(std::move(ch)); });
    unclaimed.listen({scheme, "127.0.0.1:0"});
    held_side.dial({scheme, loopback(unclaimed.port())});
    run_window(io, [&held] { held.feed(); });
}

template<typename Channel, typename Unclaimed, typename Held>
void run_dial_unclaimed(::asio::io_context &io, Unclaimed &unclaimed, Held &held_side, holder<Channel> &held, const std::string &scheme)
{
    held_side.on_accepted([&held](std::unique_ptr<Channel> ch) { held.take(std::move(ch)); });
    held_side.listen({scheme, "127.0.0.1:0"});
    unclaimed.dial({scheme, loopback(held_side.port())});
    run_window(io, [&held] { held.feed(); });
}

}

#endif

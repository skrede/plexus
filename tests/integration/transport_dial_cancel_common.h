#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TRANSPORT_DIAL_CANCEL_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TRANSPORT_DIAL_CANCEL_COMMON_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/ip/udp.hpp>
#include <asio/io_context.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <system_error>

namespace transport_dial_cancel {

// Every channel a transport handed over is held, so none is destroyed under its own pending read.
template<typename Channel>
struct outcome
{
    std::vector<std::unique_ptr<Channel>> dialed;
    std::vector<std::unique_ptr<Channel>> accepted;
    std::vector<plexus::io::io_error> failures;

    template<typename Transport>
    void record(Transport &t)
    {
        t.on_dialed([this](std::unique_ptr<Channel> ch, const plexus::io::endpoint &) { dialed.push_back(std::move(ch)); });
        t.on_dial_failed([this](const plexus::io::endpoint &, plexus::io::io_error e) { failures.push_back(e); });
        t.on_accepted([this](std::unique_ptr<Channel> ch) { accepted.push_back(std::move(ch)); });
    }
};

// listen(0) plus one connection nobody accepts fills the accept queue, so the kernel drops the next SYN and that
// connect stays pending; a host that refuses instead is reported by hold_pending.
struct full_backlog
{
    ::asio::ip::tcp::acceptor acceptor;
    ::asio::ip::tcp::socket filler;

    explicit full_backlog(::asio::io_context &io)
            : acceptor(io)
            , filler(io)
    {
        const ::asio::ip::tcp::endpoint ep{::asio::ip::make_address("127.0.0.1"), 0};
        acceptor.open(ep.protocol());
        acceptor.bind(ep);
        acceptor.listen(0);
        filler.connect(acceptor.local_endpoint());
    }

    std::string address() const
    {
        return "127.0.0.1:" + std::to_string(acceptor.local_endpoint().port());
    }
};

// A bound UDP socket that never reads, so a dial to it sends its first handshake flight and hears nothing back.
struct silent_udp_peer
{
    ::asio::ip::udp::socket socket;

    explicit silent_udp_peer(::asio::io_context &io)
            : socket(io, ::asio::ip::udp::endpoint{::asio::ip::make_address("127.0.0.1"), 0})
    {
    }

    std::string address() const
    {
        return "127.0.0.1:" + std::to_string(socket.local_endpoint().port());
    }

    bool heard() const
    {
        std::error_code ec;
        return socket.available(ec) > 0 && !ec;
    }
};

template<typename Ready>
bool run_until(::asio::io_context &io, Ready ready)
{
    const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds{2};
    while(!ready() && std::chrono::steady_clock::now() < bound)
    {
        io.restart();
        io.run_for(std::chrono::milliseconds{5});
    }
    return ready();
}

template<typename Channel>
void hold_pending(::asio::io_context &io, const outcome<Channel> &seen)
{
    io.run_for(std::chrono::milliseconds{100});
    if(!seen.dialed.empty() || !seen.failures.empty())
        SKIP("this host answered a connect over a full accept queue, so no connect stays pending");
}

inline void drain(::asio::io_context &io)
{
    io.restart();
    io.run_for(std::chrono::milliseconds{200});
}

template<typename Socket>
bool closed_by_peer(Socket &s)
{
    std::array<std::byte, 1> byte{};
    std::error_code ec;
    s.non_blocking(true);
    (void)s.read_some(::asio::buffer(byte), ec);
    return ec == ::asio::error::eof || ec == ::asio::error::connection_reset;
}

}

#endif

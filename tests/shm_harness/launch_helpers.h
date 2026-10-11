#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_LAUNCH_HELPERS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_LAUNCH_HELPERS_H

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <chrono>
#include <thread>
#include <vector>
#include <cstdint>

namespace shm_harness {

using namespace std::chrono_literals;

// The acceptors stay open until every port is read, so the returned ports are distinct.
inline std::vector<std::uint16_t> free_tcp_ports(std::uint32_t count)
{
    ::asio::io_context io;
    std::vector<::asio::ip::tcp::acceptor> acceptors;
    std::vector<std::uint16_t> ports;
    for(std::uint32_t i = 0; i < count; ++i)
    {
        acceptors.emplace_back(io, ::asio::ip::tcp::endpoint(::asio::ip::make_address("127.0.0.1"), 0));
        ports.push_back(acceptors.back().local_endpoint().port());
    }
    return ports;
}

template<typename Pred>
bool wait_until(Pred pred, std::chrono::milliseconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    return pred();
}

}

#endif

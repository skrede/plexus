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

// Each port stays bound with SO_REUSEADDR and never listens, so Linux keeps it out of port-0 binds and connect local
// ports while the node's own SO_REUSEADDR listener still binds it. A Linux-only interim until nodes listen on port 0
// and publish their bound port.
class port_reservation
{
public:
    explicit port_reservation(std::uint32_t count)
            : m_io()
            , m_ports()
            , m_held()
    {
        for(std::uint32_t i = 0; i < count; ++i)
        {
            ::asio::ip::tcp::acceptor &held = m_held.emplace_back(m_io, ::asio::ip::tcp::v4());
            held.set_option(::asio::socket_base::reuse_address(true));
            held.bind(::asio::ip::tcp::endpoint(::asio::ip::make_address("127.0.0.1"), 0));
            m_ports.push_back(held.local_endpoint().port());
        }
    }

    const std::vector<std::uint16_t> &ports() const
    {
        return m_ports;
    }

private:
    ::asio::io_context m_io;
    std::vector<std::uint16_t> m_ports;
    std::vector<::asio::ip::tcp::acceptor> m_held;
};

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

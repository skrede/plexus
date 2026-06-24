#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_PUMPS_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_DTLS_PUMPS_H

#include <asio/io_context.hpp>

#include <chrono>

namespace plexus::dtls_test {

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, std::chrono::milliseconds timeout = std::chrono::milliseconds{6000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

inline void settle(::asio::io_context &io, std::chrono::milliseconds window = std::chrono::milliseconds{40})
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

#endif

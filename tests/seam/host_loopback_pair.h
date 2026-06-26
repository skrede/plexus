#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_LOOPBACK_PAIR_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_LOOPBACK_PAIR_H

#include "host_tcp_socket.h"

#if !defined(ESP_PLATFORM)

    #include <string>
    #include <utility>
    #include <optional>

namespace plexus::test {

// A connected loopback pair: binds a transient listener on 127.0.0.1:0, dials it, and returns both
// halves so a test drives two channels over one real TCP connection. The accepted end is yielded as
// a host_tcp_socket adopting the accepted fd (the transport's dial-only half never accepts).
struct loopback_pair
{
    host_tcp_socket dialed;
    host_tcp_socket accepted;
    std::string     dial_address;
};

inline std::optional<loopback_pair> make_loopback_pair()
{
    const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
    if(listener < 0)
        return std::nullopt;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    socklen_t len        = sizeof(addr);
    if(::bind(listener, reinterpret_cast<sockaddr *>(&addr), len) < 0 || ::listen(listener, 1) < 0 || ::getsockname(listener, reinterpret_cast<sockaddr *>(&addr), &len) < 0)
    {
        ::close(listener);
        return std::nullopt;
    }
    const std::string address = "127.0.0.1:" + std::to_string(ntohs(addr.sin_port));
    host_tcp_socket dialed;
    if(dialed.connect(plexus::io::endpoint{"tcp", address}))
    {
        ::close(listener);
        return std::nullopt;
    }
    const int accepted_fd = ::accept(listener, nullptr, nullptr);
    ::close(listener);
    if(accepted_fd < 0)
        return std::nullopt;
    return loopback_pair{std::move(dialed), host_tcp_socket{accepted_fd}, address};
}

}

#endif

#endif

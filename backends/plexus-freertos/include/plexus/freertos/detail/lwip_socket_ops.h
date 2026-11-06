#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_OPS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_OPS_H

#if defined(ESP_PLATFORM)

    #include "lwip/sockets.h"

    #include <string>
    #include <cstdint>
    #include <charconv>
    #include <string_view>

namespace plexus::freertos::detail {

struct host_port
{
    std::string host;
    std::uint16_t port;
};

// Split a "host:port" address (the io::endpoint::address form) into its two parts; an absent
// or unparsable port yields port 0, which connect() then rejects.
inline host_port parse_host_port(std::string_view address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string_view::npos)
        return {std::string{address}, 0};
    std::uint16_t port = 0;
    const auto tail    = address.substr(colon + 1);
    std::from_chars(tail.data(), tail.data() + tail.size(), port);
    return {std::string{address.substr(0, colon)}, port};
}

inline void set_fd_blocking(int fd, bool blocking)
{
    const int flags = lwip_fcntl(fd, F_GETFL, 0);
    lwip_fcntl(fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
}

}

#endif

#endif

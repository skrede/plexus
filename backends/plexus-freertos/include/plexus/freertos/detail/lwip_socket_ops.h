#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_OPS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_SOCKET_OPS_H

#if defined(ESP_PLATFORM)

    #include "lwip/sockets.h"

    #include <cerrno>
    #include <chrono>
    #include <string>
    #include <cstdint>
    #include <charconv>
    #include <optional>
    #include <string_view>
    #include <system_error>

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

inline std::optional<sockaddr_in> ipv4_address(const host_port &parsed)
{
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = lwip_htons(parsed.port);
    if(inet_pton(AF_INET, parsed.host.c_str(), &addr.sin_addr) != 1)
        return std::nullopt;
    return addr;
}

// lwIP clears the pending error when SO_ERROR is read, so a caller must keep the first value.
inline std::optional<std::error_code> poll_connect(int fd)
{
    fd_set writable;
    fd_set failed;
    FD_ZERO(&writable);
    FD_ZERO(&failed);
    FD_SET(fd, &writable);
    FD_SET(fd, &failed);
    timeval zero{};
    if(lwip_select(fd + 1, nullptr, &writable, &failed, &zero) <= 0)
        return std::nullopt;
    int err       = 0;
    socklen_t len = sizeof(err);
    if(lwip_getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0)
        err = errno;
    if(err != 0)
        return std::error_code{err, std::generic_category()};
    return std::error_code{};
}

inline void set_fd_blocking(int fd, bool blocking)
{
    const int flags = lwip_fcntl(fd, F_GETFL, 0);
    lwip_fcntl(fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
}

// ESP-IDF's lwIP builds with LWIP_SO_RCVTIMEO and the standard timeval option value.
inline void apply_receive_timeout(int fd, std::chrono::milliseconds timeout)
{
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto usec = std::chrono::duration_cast<std::chrono::microseconds>(timeout - secs);
    timeval tv{};
    tv.tv_sec  = static_cast<decltype(tv.tv_sec)>(secs.count());
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>(usec.count());
    lwip_setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

}

#endif

#endif

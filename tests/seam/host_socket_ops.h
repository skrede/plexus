#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_SOCKET_OPS_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_HOST_SOCKET_OPS_H

#if !defined(ESP_PLATFORM)

    #include <fcntl.h>
    #include <netinet/in.h>
    #include <sys/time.h>
    #include <sys/socket.h>
    #include <arpa/inet.h>
    #include <unistd.h>

    #include <atomic>
    #include <cerrno>
    #include <chrono>
    #include <string>
    #include <thread>
    #include <cstdint>
    #include <utility>
    #include <charconv>
    #include <string_view>

namespace plexus::test {

inline std::pair<std::string, std::uint16_t> split_host_port(std::string_view address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string_view::npos)
        return {std::string{address}, 0};
    std::uint16_t port = 0;
    const auto tail    = address.substr(colon + 1);
    std::from_chars(tail.data(), tail.data() + tail.size(), port);
    return {std::string{address.substr(0, colon)}, port};
}

inline int connect_ipv4(int fd, std::string_view address)
{
    const auto [host, port] = split_host_port(address);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    return ::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
}

inline void set_fd_blocking(int fd, bool blocking)
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ::fcntl(fd, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
}

inline void close_fd(int fd, std::atomic<std::thread::id> *witness)
{
    if(witness)
        witness->store(std::this_thread::get_id());
    ::close(fd);
}

inline void apply_receive_timeout(int fd, std::chrono::milliseconds timeout)
{
    const auto secs = std::chrono::duration_cast<std::chrono::seconds>(timeout);
    const auto usec = std::chrono::duration_cast<std::chrono::microseconds>(timeout - secs);
    timeval tv{};
    tv.tv_sec  = static_cast<decltype(tv.tv_sec)>(secs.count());
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>(usec.count());
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

// Mirror the on-target lwip_socket::classify_io split: SOFT (EWOULDBLOCK/EAGAIN/ENOMEM) folds to 0
// and re-arms; HARD (ECONNRESET/EPIPE) sets closed so the channel fires on_error.
inline bool soft_errno(int e)
{
    return e == EWOULDBLOCK || e == EAGAIN || e == ENOMEM;
}

inline bool hard_errno(int e)
{
    return e == ECONNRESET || e == EPIPE;
}

}

#endif

#endif

#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_PRIVATE_BIND_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UNIX_PRIVATE_BIND_H

#include "plexus/detail/socket_compat.h"

#include <asio/local/stream_protocol.hpp>

#if !defined(_WIN32)
    #include <unistd.h>
    #include <sys/un.h>
#endif

#include <cerrno>
#include <cstdio>
#include <string>
#include <cstdlib>
#include <filesystem>
#include <system_error>

namespace plexus::asio::detail {

#if !defined(_WIN32)

inline std::string private_dir_template(const std::string &path)
{
    const std::filesystem::path parent = std::filesystem::path(path).parent_path();
    return (parent.empty() ? std::string(".") : parent.string()) + "/.pxb-XXXXXX";
}

inline void set_errno_error(std::error_code &ec)
{
    ec = std::error_code(errno, std::generic_category());
}

// The socket is bound and given its mode inside a fresh 0700 directory, then renamed onto the target, so the target
// never shows wider bits than the configured mode and the process-global umask is never read or written. The
// rename also replaces a stale socket left at the target atomically.
inline bool bind_in_private_dir(::asio::local::stream_protocol::acceptor &acceptor, plexus::detail::socket_mode mode, const std::string &path, std::error_code &ec)
{
    std::string dir = private_dir_template(path);
    if(dir.size() + sizeof("/s") > sizeof(::sockaddr_un{}.sun_path))
    {
        ec = std::make_error_code(std::errc::filename_too_long);
        return false;
    }
    if(::mkdtemp(dir.data()) == nullptr)
    {
        set_errno_error(ec);
        return false;
    }
    const std::string tmp = dir + "/s";
    (void)acceptor.bind(::asio::local::stream_protocol::endpoint(tmp), ec);
    if(!ec && plexus::detail::apply_socket_mode(tmp, mode, ec) && ::rename(tmp.c_str(), path.c_str()) != 0)
        set_errno_error(ec);
    plexus::detail::remove_socket_path(tmp);
    (void)::rmdir(dir.c_str());
    return !ec;
}

#else

// Windows AF_UNIX carries no mode bits, so the target is bound directly after clearing a stale socket file.
inline bool bind_in_private_dir(::asio::local::stream_protocol::acceptor &acceptor, plexus::detail::socket_mode, const std::string &path, std::error_code &ec)
{
    plexus::detail::remove_socket_path(path);
    (void)acceptor.bind(::asio::local::stream_protocol::endpoint(path), ec);
    return !ec;
}

#endif

}

#endif

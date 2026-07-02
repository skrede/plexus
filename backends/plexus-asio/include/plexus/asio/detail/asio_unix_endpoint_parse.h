#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UNIX_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UNIX_ENDPOINT_PARSE_H

#include <asio/local/stream_protocol.hpp>

#if defined(_WIN32)
    #include <afunix.h>
#else
    #include <sys/un.h>
#endif

#include <string>
#include <system_error>

namespace plexus::asio::detail {

// An empty path, or one that would not fit in sockaddr_un::sun_path (and so silently truncate to a
// DIFFERENT target on bind/connect), sets ec and returns {} (fail closed).
inline ::asio::local::stream_protocol::endpoint parse_unix(const std::string &path, std::error_code &ec)
{
    if(path.empty() || path.size() >= sizeof(::sockaddr_un{}.sun_path))
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    return ::asio::local::stream_protocol::endpoint(path);
}

}

#endif

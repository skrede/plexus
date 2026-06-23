#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UNIX_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UNIX_ENDPOINT_PARSE_H

#include <asio/local/stream_protocol.hpp>

#include <sys/un.h>

#include <string>
#include <system_error>

namespace plexus::asio::detail {

// Parse a local-stream address into a stream_protocol::endpoint. Unlike the TCP
// parse there is no host:port split — the address IS the filesystem socket path.
// An empty path, or one that would not fit in sockaddr_un::sun_path (so it would
// silently truncate to a DIFFERENT target on bind/connect), sets ec and returns
// {} — the dial/listen path then fails closed rather than binding the wrong file.
// Shared by the listener (bind) and the transport (dial) so the validation lives
// in one place.
inline ::asio::local::stream_protocol::endpoint parse_unix(const std::string &path,
                                                           std::error_code   &ec)
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

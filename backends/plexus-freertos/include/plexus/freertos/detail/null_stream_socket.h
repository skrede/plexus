#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_NULL_STREAM_SOCKET_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_NULL_STREAM_SOCKET_H

#include "plexus/io/endpoint.h"

#include <span>
#include <cstddef>
#include <system_error>

namespace plexus::freertos::detail {

// The minimal conforming witness for the in-header byte_channel / transport_backend proofs: a
// do-nothing stream_socket so the static_asserts are self-contained (the real sockets are the lwIP
// one and the host's POSIX one). It models the seam, nothing more.
struct null_stream_socket
{
    using endpoint_type = plexus::io::endpoint;

    std::error_code connect(endpoint_type)
    {
        return {};
    }
    std::size_t send(std::span<const std::byte>)
    {
        return 0;
    }
    std::size_t recv(std::span<std::byte>)
    {
        return 0;
    }
    bool closed() const
    {
        return false;
    }
    void close()
    {
    }
};

}

#endif

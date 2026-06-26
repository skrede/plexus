#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_NULL_ACCEPTOR_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_NULL_ACCEPTOR_H

#include "plexus/io/endpoint.h"

#include <optional>
#include <system_error>

namespace plexus::freertos::detail {

// The do-nothing acceptor witness mirroring null_stream_socket: it models the acceptor seam so the
// dial-only transport (the default, which never listens) and the in-header transport_backend proof
// are self-contained. accept_one() never yields a connection, so poll() drives nothing.
template<typename Socket>
struct null_acceptor
{
    std::error_code bind_and_listen(plexus::io::endpoint)
    {
        return {};
    }
    std::optional<Socket> accept_one()
    {
        return std::nullopt;
    }
    void close()
    {
    }
};

}

#endif

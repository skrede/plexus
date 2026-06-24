#ifndef HPP_GUARD_PLEXUS_ASIO_ALL_BACKENDS_MUX_H
#define HPP_GUARD_PLEXUS_ASIO_ALL_BACKENDS_MUX_H

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_channel.h"
#include "plexus/tls/tls_transport.h"
#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/muxify.h"

namespace plexus::asio {

using all_backends_mux = io::multiplexing_transport<unix_transport, asio_transport, tls::tls_transport, udp_transport, tls::dtls_transport>;

// The members are BORROWED (held by reference) and MUST outlive the returned object — the
// owner sequences teardown so the mux is destroyed before any member fires a late completion.
inline all_backends_mux make_all_backends_mux(unix_transport &local, asio_transport &remote, tls::tls_transport &secure, udp_transport &datagram, tls::dtls_transport &secure_datagram,
                                              io::transport_selector selector = {})
{
    return all_backends_mux{local, remote, secure, datagram, secure_datagram, selector};
}

}

static_assert(plexus::io::transport_backend<plexus::asio::all_backends_mux, plexus::muxify<plexus::asio::asio_policy>>,
              "all_backends_mux must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

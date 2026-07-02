#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_ALL_BACKENDS_MUX_SHM_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_ALL_BACKENDS_MUX_SHM_H

#include "plexus/asio/shm/windows/shm_member.h"

#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/tls/tls_transport.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/muxify.h"

namespace plexus::asio::shm {

using all_backends_mux_shm = io::multiplexing_transport<shm_member, unix_transport, asio_transport, tls::tls_transport, udp_transport, tls::dtls_transport>;

// Injects the same-host preference hook (prefer shm, fall back to AF_UNIX on a failed ring
// acquire). The members are BORROWED and MUST outlive the returned object.
inline all_backends_mux_shm make_all_backends_mux_shm(shm_member &shm, unix_transport &local, asio_transport &remote, tls::tls_transport &secure, udp_transport &datagram,
                                                      tls::dtls_transport &secure_datagram, io::transport_selector selector = {})
{
    return all_backends_mux_shm{shm, local, remote, secure, datagram, secure_datagram, selector, ::plexus::shm::prefer_upgradeable_hook(shm)};
}

}

static_assert(plexus::io::transport_backend<plexus::asio::shm::all_backends_mux_shm, plexus::muxify<plexus::asio::asio_policy>>,
              "all_backends_mux_shm must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

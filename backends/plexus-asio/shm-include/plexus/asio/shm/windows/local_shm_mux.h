#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_LOCAL_SHM_MUX_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_LOCAL_SHM_MUX_H

#include "plexus/asio/shm/windows/shm_member.h"

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/muxify.h"

namespace plexus::asio::shm {

// The crypto-free counterpart to all_backends_mux_shm (shm + AF_UNIX + plain TCP): it pulls in no
// tls/dtls/udp/openssl. shm is the FIRST positional member so the preference hook (which scans
// candidates for the local_fast_eligible flag) finds it.
using local_shm_mux = io::multiplexing_transport<shm_member, unix_transport, asio_transport>;

// Injects the same-host preference hook (prefer shm, fall back to AF_UNIX on a failed ring
// acquire). The members are BORROWED and MUST outlive the returned object.
inline local_shm_mux make_local_shm_mux(shm_member &shm, unix_transport &local, asio_transport &remote, io::transport_selector selector = {})
{
    return local_shm_mux{shm, local, remote, selector, ::plexus::shm::prefer_upgradeable_hook(shm)};
}

}

static_assert(plexus::io::transport_backend<plexus::asio::shm::local_shm_mux, plexus::muxify<plexus::asio::asio_policy>>,
              "local_shm_mux must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

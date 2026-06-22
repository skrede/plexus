#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_LOCAL_SHM_MUX_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_LOCAL_SHM_MUX_H

#include "plexus/asio/shm/linux/shm_member.h"

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/muxify.h"

namespace plexus::asio::shm {

// The lean same-host composition: shared memory + AF_UNIX + plain TCP, WITHOUT the secure
// (TLS/DTLS) or datagram (UDP) members. This is the crypto-free counterpart to
// all_backends_mux_shm — it includes only shm_member.h + the AF_UNIX and plain-TCP stream
// transports, so it pulls in no tls/dtls/udp/openssl. A consumer that needs the local fast
// path plus a plain-TCP remote, but no TLS, composes this and builds with the crypto
// backend disabled. shm is the FIRST positional member so the preference hook (which scans
// candidates for the shm_eligible flag) finds it, AF_UNIX is the same-host fallback, and
// plain TCP is the cross-host stream. shm_member + make_shm_member come from shm_member.h.
using local_shm_mux = io::multiplexing_transport<shm_member, unix_transport, asio_transport>;

// Construct the lean shm-bearing multiplexer over three caller-owned member transports,
// injecting the same-host preference hook (prefer shm, fall back to AF_UNIX on a failed
// ring acquire). The members are BORROWED and MUST outlive the returned object. Returns a
// prvalue (guaranteed elision) so the member callbacks capture the final address.
[[nodiscard]] inline local_shm_mux make_local_shm_mux(shm_member &shm, unix_transport &local,
                                                      asio_transport        &remote,
                                                      io::transport_selector selector = {})
{
    return local_shm_mux{shm, local, remote, selector, io::prefer_upgradeable_hook(shm)};
}

}

static_assert(plexus::io::transport_backend<plexus::asio::shm::local_shm_mux,
                                            plexus::muxify<plexus::asio::asio_policy>>,
              "local_shm_mux must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

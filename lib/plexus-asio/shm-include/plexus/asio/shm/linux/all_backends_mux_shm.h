#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_ALL_BACKENDS_MUX_SHM_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_ALL_BACKENDS_MUX_SHM_H

#include "plexus/asio/shm/linux/shm_member.h"

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

// The shm-bearing composition: the all-backends multiplexer WITH shared memory joining
// as the SECOND same-host (local-tier) member alongside AF_UNIX. The local tier now
// resolves to >1 candidate (shm + unix), so the preference hook becomes load-bearing --
// prefer shared memory when the pair is same-host AND the ring acquire succeeds, else
// fall back to AF_UNIX. This is an ADDITIVE alias: the no-shm all_backends_mux stays a
// valid composition for a node that omits shared memory. shm_member + make_shm_member (the
// broker + reactor-bridge binder recipe) come from shm_member.h.
using all_backends_mux_shm = io::multiplexing_transport<shm_member, unix_transport,
                                                        asio_transport, tls::tls_transport,
                                                        udp_transport, tls::dtls_transport>;

// Construct the shm-bearing multiplexer over six caller-owned member transports, injecting
// the same-host preference hook (prefer shm, fall back to AF_UNIX on a failed ring acquire).
// The members are BORROWED and MUST outlive the returned object. Returns a prvalue (guaranteed
// elision) so the member callbacks capture the final address.
[[nodiscard]] inline all_backends_mux_shm make_all_backends_mux_shm(
        shm_member &shm, unix_transport &local, asio_transport &remote, tls::tls_transport &secure,
        udp_transport &datagram, tls::dtls_transport &secure_datagram,
        io::transport_selector selector = {})
{
    return all_backends_mux_shm{shm, local, remote, secure, datagram, secure_datagram, selector,
                                io::shm::prefer_shm_hook(shm)};
}

}

static_assert(plexus::io::transport_backend<plexus::asio::shm::all_backends_mux_shm,
                                            plexus::muxify<plexus::asio::asio_policy>>,
    "all_backends_mux_shm must satisfy transport_backend — check the listen/dial/on_* surface");

#endif

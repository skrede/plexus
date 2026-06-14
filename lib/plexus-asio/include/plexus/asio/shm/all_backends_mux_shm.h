#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_ALL_BACKENDS_MUX_SHM_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_ALL_BACKENDS_MUX_SHM_H

#include "plexus/asio/shm/ring_notifier.h"

#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/shm/posix_shm_region_broker.h"

#include "plexus/tls/tls_transport.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/io/shm/shm_mux_member.h"

#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/muxify.h"

#include <atomic>
#include <cstdint>
#include <optional>
#include <utility>

namespace plexus::asio::shm {

// The shm-bearing composition: the all-backends multiplexer WITH shared memory joining
// as the SECOND same-host (local-tier) member alongside AF_UNIX. The local tier now
// resolves to >1 candidate (shm + unix), so the preference hook becomes load-bearing --
// prefer shared memory when the pair is same-host AND the ring acquire succeeds, else
// fall back to AF_UNIX. This is an ADDITIVE alias: the no-shm all_backends_mux stays a
// valid composition for a node that omits shared memory.
//
// The shm member is built over the POSIX broker + the io_uring-futex reactor bridge
// (ring_notifier), the only asio-coupled shared-memory piece. The bridge is NOT default-
// constructible (it binds to the ring's generation word + the user's io_context), so the
// member's registry takes a notifier-binder that captures the io_context and emplaces the
// bridge over (executor, word) once each ring is bound. The member is the FIRST positional
// member so the preference hook (which scans candidates for the shm_eligible flag) finds it.
using shm_member = io::shm::shm_mux_member<::plexus::shm::posix_shm_region_broker,
                                           ring_notifier<muxify<asio_policy>>>;

using all_backends_mux_shm = io::multiplexing_transport<shm_member, unix_transport,
                                                        asio_transport, tls::tls_transport,
                                                        udp_transport, tls::dtls_transport>;

// The notifier-binder for the bridge: capture the io_context and emplace a ring_notifier
// over (executor, word) once the registry binds each ring. Free function so the lambda
// shape lives in one place.
[[nodiscard]] inline io::shm::shm_topic_registry<::plexus::shm::posix_shm_region_broker,
                                                 ring_notifier<muxify<asio_policy>>>::notifier_binder
make_bridge_binder(::asio::io_context &io)
{
    return [&io](std::optional<ring_notifier<muxify<asio_policy>>> &slot, std::atomic<std::uint32_t> &word,
                 std::atomic<std::uint32_t> &park) {
        slot.emplace(io, word, park);
    };
}

// Construct the shm member over the POSIX broker + the reactor bridge bound to `io`. The
// broker is borrowed (it must outlive the member); the binder captures `io` so each ring's
// notifier wakes on that reactor. The member is then composed as the first local candidate.
[[nodiscard]] inline shm_member make_shm_member(::asio::io_context &io,
                                                ::plexus::shm::posix_shm_region_broker &broker,
                                                io::reliability rel = io::reliability::reliable,
                                                io::congestion cong = io::congestion::block)
{
    return shm_member{broker, rel, cong, make_bridge_binder(io)};
}

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

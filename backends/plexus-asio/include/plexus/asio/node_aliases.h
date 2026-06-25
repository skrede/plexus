#ifndef HPP_GUARD_PLEXUS_ASIO_NODE_ALIASES_H
#define HPP_GUARD_PLEXUS_ASIO_NODE_ALIASES_H

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"

// PLEXUS_SAME_HOST_NO_SHM forces the portable AF_UNIX + TCP branch on any host (it lets the
// non-shm composition be exercised off a Linux build host).
#if defined(__linux__) && !defined(PLEXUS_SAME_HOST_NO_SHM)
    #define PLEXUS_SAME_HOST_SHM 1
#else
    #define PLEXUS_SAME_HOST_SHM 0
#endif

#if PLEXUS_SAME_HOST_SHM
    #include "plexus/publisher.h"

    #include "plexus/asio/shm/linux/shm_member.h"

    #include "plexus/shm/ring_geometry_mode.h"

    #include <string_view>
#endif

namespace plexus::asio {

// The common single-wire asio composition.
using node = basic_node<asio_policy, asio_transport>;

#if PLEXUS_SAME_HOST_SHM
namespace shm {

// The shm member leads the pack: the same-host preference hook scans for the first
// local_fast_eligible candidate, so the shm leaf must precede the unix and tcp fallbacks.
using node = basic_node<asio_policy, shm_member, unix_transport, asio_transport>;

// The typed same-host-geometry advertise front-door. The generic api carries the per-topic ring
// override as an opaque const void* (it names no shm type); this backend front-door is the one
// place that names the concrete ::plexus::shm::shm_geometry, bridging &geom into the opaque
// typed_publisher_options::geometry before delegating to the generic advertise path. The geometry
// is a producer-side local value consumed only for the synchronous declare turn.
template<typename Codec, typename Node>
publisher<Codec> advertise(Node &n, std::string_view topic, ::plexus::shm::shm_geometry geom, typed_publisher_options opts = {}, Codec codec = {})
{
    opts.geometry = &geom;
    return n.template advertise<Codec>(topic, opts, std::move(codec));
}

}
#endif

}

#endif

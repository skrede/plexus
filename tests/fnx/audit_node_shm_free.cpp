// The shm-free node-surface compile-gate translation unit.
//
// This TU pulls the full node facade (node.h) and instantiates a NON-shm node over
// the inproc policy + inproc_transport. Its value is STRUCTURAL: it must BUILD. A
// non-shm node carries no same-host shared-memory member, so node.h's transitive
// include graph for this composition must reach no member header that pulls the
// 32-bit ring_layout static_assert (shm_mux_member.h / ring_geometry.h). The same-host
// wiring is duck-typed over the member type, so those branches are never instantiated
// for an inproc node and the shm member header is never needed at the node surface.
//
// The proof is structural by absence: this TU deliberately includes no shm transport
// header, so the shm member type is out of scope here. A build that silently depended
// on it would fail. The load-bearing textual assertion is the include-audit grep over
// node.h / node_upgrade_wiring.h; this TU instantiates the full non-shm node + its
// routing_engine under the build-fnx -fno-exceptions flags to prove the instantiation
// compiles shm-free.

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/node_id.h"

#include <cstddef>

int main()
{
    using policy    = plexus::inproc::inproc_policy;
    using transport = plexus::inproc::inproc_transport<>;
    using node_t    = plexus::node<policy, transport>;

    // The full non-shm node composes a routing_engine over the inproc substrate; name
    // it so the engine type the node owns is instantiated explicitly on the floor too.
    using engine_t = node_t::engine_type;
    static_assert(std::is_same_v<engine_t, plexus::io::routing_engine<policy, transport>>, "a single-transport node composes the engine directly over its policy");

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> executor{bus};
    transport tr{executor, bus};
    plexus::discovery::static_discovery disc{{}};

    plexus::node_id self{};
    self[0] = std::byte{0x01};

    plexus::node_options opts;

    // Construct + destruct: the ctor stands up the engine and the same-host wiring's
    // if-constexpr capability checks resolve to no-ops for the inproc member (no shm
    // member is present), proving the wiring compiles with no shm header in scope.
    {
        node_t node{executor, disc, self, tr, opts};
    }

    return 0;
}

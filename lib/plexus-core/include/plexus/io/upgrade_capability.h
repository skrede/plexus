#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_CAPABILITY_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_CAPABILITY_H

#include "plexus/io/endpoint.h"

#include <concepts>

namespace plexus::io {

// The generic same-medium upgrade capability. A transport member satisfies it iff it can probe
// whether a higher-priority medium is acquirable for an endpoint. A member without the probe
// (a wire-only transport) simply does not satisfy it, so the conditional upgrade wiring is never
// instantiated for it. This is the named generalization of the duck-typed node_has_can_acquire.
template<typename M>
concept upgradeable = requires(M &m, const endpoint &ep) {
    { m.can_acquire(ep) } -> std::convertible_to<bool>;
};

}

#endif

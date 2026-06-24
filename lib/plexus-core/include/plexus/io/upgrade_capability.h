#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_CAPABILITY_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_CAPABILITY_H

#include "plexus/io/endpoint.h"

#include <concepts>

namespace plexus::io {

template<typename M>
concept upgradeable = requires(M &m, const endpoint &ep) {
    { m.can_acquire(ep) } -> std::convertible_to<bool>;
};

}

#endif

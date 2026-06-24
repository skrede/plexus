#ifndef HPP_GUARD_PLEXUS_EXPECTED_H
#define HPP_GUARD_PLEXUS_EXPECTED_H

#include "plexus/detail/expected.h"

namespace plexus {

template<typename T, typename E>
using expected = detail::expected<T, E>;

template<typename E>
using unexpected = detail::unexpected<E>;

using detail::unexpect_t;
using detail::unexpect;

}

#endif

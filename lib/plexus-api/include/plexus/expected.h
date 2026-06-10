#ifndef HPP_GUARD_PLEXUS_EXPECTED_H
#define HPP_GUARD_PLEXUS_EXPECTED_H

#include "plexus/detail/expected.h"

namespace plexus {

// The public result vocabulary. These aliases ARE the stable names; the eventual
// std::expected cutover is a one-line swap in detail/expected.h, invisible here.
template <typename T, typename E>
using expected = detail::expected<T, E>;

template <typename E>
using unexpected = detail::unexpected<E>;

using detail::unexpect_t;
using detail::unexpect;

}

#endif

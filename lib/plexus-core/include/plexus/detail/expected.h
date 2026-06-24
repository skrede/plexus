#ifndef HPP_GUARD_PLEXUS_DETAIL_EXPECTED_H
#define HPP_GUARD_PLEXUS_DETAIL_EXPECTED_H

#include <version>

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L

    #include <expected>

namespace plexus::detail {

template<typename T, typename E>
using expected = std::expected<T, E>;

template<typename E>
using unexpected = std::unexpected<E>;

using unexpect_t = std::unexpect_t;
inline constexpr unexpect_t unexpect{std::unexpect};

}

#else

    #include "plexus/detail/expected_fallback.h"

#endif

#endif

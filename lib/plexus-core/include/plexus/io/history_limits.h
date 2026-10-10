#ifndef HPP_GUARD_PLEXUS_IO_HISTORY_LIMITS_H
#define HPP_GUARD_PLEXUS_IO_HISTORY_LIMITS_H

#include <cstddef>

namespace plexus::io {

// DoS clamp: a publisher-declared depth is attacker-controlled (bounds N x max_payload).
constexpr std::size_t k_history_depth_cap = 1024;
constexpr std::size_t k_fetch_cap         = 1024;

}

#endif

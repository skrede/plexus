#ifndef HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_BOUNDS_H
#define HPP_GUARD_PLEXUS_MATCH_KEY_PATTERN_BOUNDS_H

#include <cstddef>

namespace plexus::match
{

// The default ceilings a power user overrides with an alternative traits type carrying the same two
// members. The 256-byte length sits deliberately below the 1024-byte wire fqn lid: real topic names
// are far shorter, a 256-byte inline buffer is MCU-affordable, and a wire-legal 257..1024-byte name
// is adversarial territory that construction refuses and counts rather than stores.
struct default_bounds
{
    static constexpr std::size_t k_pattern_depth  = 16;
    static constexpr std::size_t k_pattern_length = 256;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DEMAND_TRANSITION_H
#define HPP_GUARD_PLEXUS_IO_DEMAND_TRANSITION_H

#include <cstdint>

namespace plexus::io {

// Only the two boundary crossings emit; intermediate refcount edges (1->2, 2->1) do not.
enum class demand_transition : std::uint8_t
{
    up,
    down,
};

enum class demand_role : std::uint8_t
{
    publisher,
    subscriber,
};

}

#endif

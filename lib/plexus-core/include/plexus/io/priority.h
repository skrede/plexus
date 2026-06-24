#ifndef HPP_GUARD_PLEXUS_IO_PRIORITY_H
#define HPP_GUARD_PLEXUS_IO_PRIORITY_H

#include <cstdint>

namespace plexus::io {

// A local egress-ordering selector, strictly off-wire (it rides no frame field), so
// the values are renumbered freely.
enum class priority : std::uint8_t
{
    background = 0,
    normal     = 1,
    high       = 2,
    realtime   = 3,
};

}

#endif

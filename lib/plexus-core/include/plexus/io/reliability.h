#ifndef HPP_GUARD_PLEXUS_IO_RELIABILITY_H
#define HPP_GUARD_PLEXUS_IO_RELIABILITY_H

#include <cstdint>

namespace plexus::io {

enum class reliability : std::uint8_t
{
    best_effort,
    reliable,
};

}

#endif

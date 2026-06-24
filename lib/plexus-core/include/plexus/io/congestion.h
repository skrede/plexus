#ifndef HPP_GUARD_PLEXUS_IO_CONGESTION_H
#define HPP_GUARD_PLEXUS_IO_CONGESTION_H

#include <cstdint>

namespace plexus::io {

// A local back-pressure choice, off-wire (never serialized), so the values are
// renumbered freely.
enum class congestion : std::uint8_t
{
    block,
    drop_oldest,
    drop_newest,
};

}

#endif

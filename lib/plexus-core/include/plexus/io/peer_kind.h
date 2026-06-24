#ifndef HPP_GUARD_PLEXUS_IO_PEER_KIND_H
#define HPP_GUARD_PLEXUS_IO_PEER_KIND_H

#include <cstdint>

namespace plexus::io {

enum class peer_kind : std::uint8_t
{
    dialed,
    accepted
};

}

#endif

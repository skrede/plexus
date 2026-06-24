#ifndef HPP_GUARD_PLEXUS_NODE_ID_H
#define HPP_GUARD_PLEXUS_NODE_ID_H

#include <array>
#include <cstddef>

namespace plexus {

// A type alias, not a wrapper: equality and unsigned-lexicographic ordering come
// from std::array's defaulted operator<=> over std::byte, and wire/handshake.h can
// carry the raw std::array<std::byte, 16> without including this header.
using node_id = std::array<std::byte, 16>;

}

#endif

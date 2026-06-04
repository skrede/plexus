#ifndef HPP_GUARD_PLEXUS_NODE_ID_H
#define HPP_GUARD_PLEXUS_NODE_ID_H

#include <array>
#include <cstddef>

namespace plexus {

// The stable participant identity: an opaque, fixed-size, network-unique 128-bit
// value. Derivation-neutral — a node may derive it from a UUID, a host+pid+counter,
// a hash, etc.; plexus only COMPARES it (equality for identity, unsigned-
// lexicographic for dedup), never interprets or generates it. It is user-provided,
// required, with no default — plexus never mints one.
//
// A type ALIAS, not a wrapper struct: comparison and ordering come for free from
// std::array's defaulted operator<=> over std::byte (an enum class : unsigned char,
// so the ordering is unsigned-lexicographic — exactly the dedup contract). The
// alias lets wire/handshake.h carry the field as the raw std::array<std::byte, 16>
// WITHOUT including this header (plexus-wire stays zero-upward-dependency), with
// full type transparency at the consuming core.
using node_id = std::array<std::byte, 16>;

}

#endif

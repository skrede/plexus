#ifndef HPP_GUARD_PLEXUS_WIRE_TOPIC_DECLARATION_H
#define HPP_GUARD_PLEXUS_WIRE_TOPIC_DECLARATION_H

#include "plexus/wire/subscribe.h"

#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::wire {

// The type assertion is three-state, and a uint16-length-prefixed name cannot separate "no type
// asserted" from "asserted the empty name" by length alone — so the state rides its own byte.
enum class type_state : std::uint8_t
{
    undeclared     = 0,
    declared_empty = 1,
    declared       = 2
};

// A producer's topic-with-type, asserted in-band on an authenticated session — the discovery
// broadcast carries identity and ports only, never topic identity. type_id is the equality token a
// receiver compares to detect producers disagreeing over a topic's type; type_name is the opaque
// name plexus carries for enumeration and never parses. The fqn rides beside the hash it hashes to
// because a hash is one-way: a receiver enumerating a topic it never subscribed to has no other
// source for the name (the same reason subscribe_request carries both).
struct topic_declaration
{
    std::uint64_t topic_hash;
    std::uint64_t type_id;
    std::string fqn;
    std::string type_name;
    type_state state;

    friend bool operator==(const topic_declaration &, const topic_declaration &) = default;
};

namespace detail {

// Wire layout: topic_hash(8) + type_id(8) + type_state(1) + fqn_len(2) + fqn_bytes
//   + type_name_len(2) + type_name_bytes.
// Both strings reuse subscribe's per-callsite lids — one bound per named thing, never re-minted.
constexpr std::size_t topic_declaration_min_size = 21;

}

}

#include "plexus/wire/detail/topic_declaration_codec.h"

#endif

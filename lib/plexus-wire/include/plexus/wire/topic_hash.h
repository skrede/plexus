#ifndef HPP_GUARD_PLEXUS_WIRE_TOPIC_HASH_H
#define HPP_GUARD_PLEXUS_WIRE_TOPIC_HASH_H

#include <cstdint>
#include <string_view>

namespace plexus::wire {

// Stable fqn -> topic_hash. plexus carries no hashing dependency, so the wire
// identity is computed with 64-bit FNV-1a: deterministic for identical bytes on
// every platform (unlike std::hash, which is toolchain-defined), which is the
// only correct property for an identifier two peers must agree on. The message
// and procedure forwarders both route through this one definition, so their
// resolve-by-hash stays self-consistent on the same fqn.
inline std::uint64_t fqn_topic_hash(std::string_view fqn) noexcept
{
    std::uint64_t h = 1469598103934665603ull;
    for(char c : fqn)
    {
        h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        h *= 1099511628211ull;
    }
    return h;
}

}

#endif

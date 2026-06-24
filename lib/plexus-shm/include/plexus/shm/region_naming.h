#ifndef HPP_GUARD_PLEXUS_SHM_REGION_NAMING_H
#define HPP_GUARD_PLEXUS_SHM_REGION_NAMING_H

#include "plexus/wire/topic_hash.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace plexus::shm {

// A pub/sub topic uses request as its single direction; a req/res pair names two
// distinct rings (one per direction) off the same fqn so the flows never share a ring.
enum class ring_direction : std::uint8_t
{
    request,
    response,
};

inline std::string bare_hex(std::uint64_t h)
{
    static constexpr char k_digits[] = "0123456789abcdef";
    std::string out(16, '0');
    for(int i = 15; i >= 0; --i)
    {
        out[static_cast<std::size_t>(i)] = k_digits[h & 0xfu];
        h >>= 4;
    }
    return out;
}

// The deterministic region name both ends compute INDEPENDENTLY with no exchange: each
// hashes the same fqn through the same cross-process-stable fqn hash and meets at the
// same name. The response direction and a non-empty region_ns each fold a unit-separator
// (0x1f, which cannot occur in an fqn, so it never aliases a real topic) discriminator
// into the hashed key before rendering. An empty namespace is byte-identical to the
// namespace-less name.
inline std::string region_name_for(std::string_view fqn, ring_direction direction, std::string_view region_ns = "")
{
    if(region_ns.empty() && direction == ring_direction::request)
        return bare_hex(plexus::wire::fqn_topic_hash(fqn));

    std::string keyed;
    if(!region_ns.empty())
    {
        keyed += region_ns;
        keyed.push_back('\x1f');
    }
    keyed += fqn;
    if(direction == ring_direction::response)
    {
        keyed.push_back('\x1f');
        keyed += "response";
    }
    return bare_hex(plexus::wire::fqn_topic_hash(keyed));
}

}

#endif

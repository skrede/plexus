#ifndef HPP_GUARD_PLEXUS_SHM_REGION_NAMING_H
#define HPP_GUARD_PLEXUS_SHM_REGION_NAMING_H

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <string>
#include <cstddef>
#include <cstdint>
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

namespace detail {

inline void append_be32(std::string &key, std::uint32_t v)
{
    for(int shift = 24; shift >= 0; shift -= 8)
        key.push_back(static_cast<char>((v >> shift) & 0xffu));
}

}

// The deterministic region name both ends compute independently with no exchange. The hashed key is, in this fixed
// order: the fqn's length (4 big-endian bytes) and bytes, the publisher's 16 id bytes, the universe (4 big-endian
// bytes), the namespace's length and bytes, then one direction byte. Fixed widths and length prefixes make the key
// injective, and only its 64-bit hash is rendered, so every name is 16 hex characters.
inline std::string region_name_for(std::string_view fqn, const node_id &publisher, std::uint32_t universe, ring_direction direction, std::string_view region_ns)
{
    std::string key;
    key.reserve(fqn.size() + region_ns.size() + publisher.size() + 13);
    detail::append_be32(key, static_cast<std::uint32_t>(fqn.size()));
    key.append(fqn);
    for(const std::byte b : publisher)
        key.push_back(static_cast<char>(b));
    detail::append_be32(key, universe);
    detail::append_be32(key, static_cast<std::uint32_t>(region_ns.size()));
    key.append(region_ns);
    key.push_back(static_cast<char>(direction));
    return bare_hex(plexus::wire::fqn_topic_hash(key));
}

}

#endif

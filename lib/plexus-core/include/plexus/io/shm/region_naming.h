#ifndef HPP_GUARD_PLEXUS_IO_SHM_REGION_NAMING_H
#define HPP_GUARD_PLEXUS_IO_SHM_REGION_NAMING_H

#include "plexus/wire/topic_hash.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace plexus::io::shm {

// The direction of a same-host ring relative to a request/response exchange. A
// pub/sub topic uses request as its single direction; a req/res pair names two
// distinct rings (one per direction) off the same fqn so the two flows never share
// a ring.
enum class ring_direction : std::uint8_t
{
    request,
    response,
};

// Render a 64-bit hash as bare lowercase hex (no "0x", no separators) — the
// region-name body. Bare hex keeps the name a clean POSIX shm-object token both
// ends compute identically.
[[nodiscard]] inline std::string bare_hex(std::uint64_t h)
{
    static constexpr char k_digits[] = "0123456789abcdef";
    std::string           out(16, '0');
    for(int i = 15; i >= 0; --i)
    {
        out[static_cast<std::size_t>(i)] = k_digits[h & 0xfu];
        h >>= 4;
    }
    return out;
}

// The deterministic region name for a (fqn, direction) pair, computed INDEPENDENTLY
// by both ends with NO exchange: both sides hash the same fqn through the same
// cross-process-stable fqn hash and meet at the same name. The response direction
// folds a unit-separator + "response" discriminator into the hash BEFORE rendering,
// so request and response name distinct rings off one fqn. The unit separator (0x1f)
// cannot occur in an fqn, so the discriminator never aliases a real topic.
//
// region_ns is a static shm-region NAMESPACE both peers set identically (local
// config, never wire-advertised): a NON-empty namespace is folded into the hashed
// key BEFORE the fqn (separated by the same 0x1f unit separator) so two UNRELATED
// applications that pick distinct namespaces compute DISTINCT region names for the
// same topic. An EMPTY namespace is the no-fold default — byte-identical to the
// namespace-less name.
[[nodiscard]] inline std::string region_name_for(std::string_view fqn, ring_direction direction,
                                                 std::string_view region_ns = "")
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

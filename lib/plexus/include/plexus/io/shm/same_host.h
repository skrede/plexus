#ifndef HPP_GUARD_PLEXUS_IO_SHM_SAME_HOST_H
#define HPP_GUARD_PLEXUS_IO_SHM_SAME_HOST_H

#include "plexus/wire/topic_hash.h"

#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace plexus::io::shm {

// The host fingerprint: a fixed-size value identifying the MACHINE a node runs on,
// DISTINCT from node_id (which identifies a participant). Two nodes on the same
// host carry the same fingerprint; two nodes on different hosts carry different
// ones. It is derived from the machine-id + hostname (a tiny platform read that
// lives in the shared-memory backend, plexus-shm/machine_fingerprint.cpp — NOT
// here: this core header only COMPARES and NAMES, never reads the platform).
//
// The fingerprint is a 64-bit value: the FNV-1a hash of the machine-id+hostname
// string (plexus carries no XXH3 dependency; the determinism + host-distinctness
// the same-host test needs are exactly what the cross-process-stable fqn hash
// already provides). A zero value is the NULL fingerprint — "no fingerprint
// computed / advertised" — which is NEVER same-host (Pitfall 2, the fail-closed
// guard below). The node computes its own fingerprint ONCE into owned state and
// passes it in; this header holds no static memoization (the no-static-singleton
// discipline).
struct host_fingerprint
{
    std::uint64_t value = 0;

    [[nodiscard]] bool is_null() const noexcept { return value == 0; }

    friend bool operator==(host_fingerprint a, host_fingerprint b) noexcept
    {
        return a.value == b.value;
    }
};

// The load-bearing null-guard (Pitfall 2): two ends are same-host iff the peer's
// fingerprint is NON-null AND equals the local one. A null peer fingerprint is
// NEVER same-host — a peer that advertises nothing (or a forged zero) cannot claim
// co-location it has not proven, so the pair falls back to the wire. This fails
// CLOSED: the equality check alone is insufficient because two null fingerprints
// would compare equal and spuriously claim same-host.
[[nodiscard]] inline bool is_same_host(host_fingerprint peer, host_fingerprint local) noexcept
{
    return !peer.is_null() && peer == local;
}

// The direction of a same-host ring relative to a request/response exchange. A
// pub/sub topic uses request as its single direction; a req/res pair names two
// distinct rings (one per direction) off the same fqn so the two flows never share
// a ring. The discriminator below renders these to distinct deterministic names.
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
    std::string out(16, '0');
    for(int i = 15; i >= 0; --i)
    {
        out[static_cast<std::size_t>(i)] = k_digits[h & 0xfu];
        h >>= 4;
    }
    return out;
}

// The deterministic region name for a (fqn, direction) pair, computed INDEPENDENTLY
// by both ends with NO exchange (demand-driven convergence): both sides hash the
// same fqn through the same cross-process-stable fqn hash and meet at the same name.
// The response direction folds a unit-separator + "response" discriminator into the
// hash BEFORE rendering, so request and response name distinct rings off one fqn
// (a req/res pair never collides its two flows onto one ring). The unit separator
// (0x1f) cannot occur in an fqn, so the discriminator never aliases a real topic.
[[nodiscard]] inline std::string region_name_for(std::string_view fqn, ring_direction direction)
{
    std::uint64_t h = plexus::wire::fqn_topic_hash(fqn);
    if(direction == ring_direction::response)
    {
        std::string keyed{fqn};
        keyed.push_back('\x1f');
        keyed += "response";
        h = plexus::wire::fqn_topic_hash(keyed);
    }
    return bare_hex(h);
}

}

#endif

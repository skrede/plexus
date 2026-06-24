#ifndef HPP_GUARD_PLEXUS_IO_HOST_FINGERPRINT_H
#define HPP_GUARD_PLEXUS_IO_HOST_FINGERPRINT_H

#include <cstdint>

namespace plexus::io {

// A fixed-size value identifying the MACHINE a node runs on, DISTINCT from node_id
// (which identifies a participant). Two nodes on the same host carry the same
// fingerprint; two on different hosts carry different ones. It is a 64-bit value
// derived from the machine-id + hostname by a backend read (the platform read lives
// in the mechanism backend, not here: this header only COMPARES and NAMES). A zero
// value is the NULL fingerprint — "no fingerprint computed / advertised" — which is
// NEVER same-host (the fail-closed null-guard below). The node computes its own
// fingerprint once into owned state and passes it in; this header holds no static
// memoization.
struct host_fingerprint
{
    std::uint64_t value = 0;

    [[nodiscard]] bool is_null() const noexcept { return value == 0; }

    friend bool operator==(host_fingerprint a, host_fingerprint b) noexcept
    {
        return a.value == b.value;
    }
};

// Two ends are same-host iff the peer's fingerprint is NON-null AND equals the local
// one. A null peer fingerprint is NEVER same-host (fail closed): the equality check
// alone is insufficient because two null fingerprints would compare equal and
// spuriously claim co-location.
[[nodiscard]] inline bool is_same_host(host_fingerprint peer, host_fingerprint local) noexcept
{
    return !peer.is_null() && peer == local;
}

}

#endif

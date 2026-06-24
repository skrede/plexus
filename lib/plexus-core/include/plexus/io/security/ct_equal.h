#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_CT_EQUAL_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_CT_EQUAL_H

#include <span>
#include <cstddef>

namespace plexus::io::security {

// The constant-time compare: OR-accumulate the per-byte difference over ALL bytes
// (no early return, no break mid-loop), then test the accumulator once. A near-miss
// leaks no timing about which byte differs. A length mismatch rejects immediately.
inline bool ct_equal(std::span<const std::byte> a, std::span<const std::byte> b) noexcept
{
    if(a.size() != b.size())
        return false;
    std::byte diff{0};
    for(std::size_t i = 0; i < a.size(); ++i)
        diff |= a[i] ^ b[i];
    return diff == std::byte{0};
}

}

#endif

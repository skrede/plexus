#ifndef HPP_GUARD_PLEXUS_SESSION_ID_H
#define HPP_GUARD_PLEXUS_SESSION_ID_H

#include <compare>
#include <cstdint>

namespace plexus {

// The per-connection session epoch: a monotonically minted u64 the staleness gate
// compares to drop frames from a superseded connection. Widened from the original
// single byte because a u8 epoch wraps at 255 reconnects, a real collision window
// for the staleness latch.
//
// A struct wrapping the raw u64, NOT a bare `using session_id = std::uint64_t;`
// alias: the three identities (node_id / session_id / publisher_gid) must stay
// distinct with no implicit conversion. The wire layer keeps the raw u64
// (frame_header.session_id) so plexus-wire stays zero-upward-dependency; the core
// wraps it at the boundary, mirroring how the core treats handshake_request.id as
// a node_id. The raw value is reached through value(); ordering and equality are
// the defaulted member-wise compare.
struct session_id
{
    std::uint64_t m_value{};

    std::uint64_t value() const noexcept
    {
        return m_value;
    }

    auto operator<=>(const session_id &) const = default;
};

}

#endif

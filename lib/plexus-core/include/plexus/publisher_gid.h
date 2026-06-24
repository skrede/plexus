#ifndef HPP_GUARD_PLEXUS_PUBLISHER_GID_H
#define HPP_GUARD_PLEXUS_PUBLISHER_GID_H

#include "plexus/node_id.h"

#include <compare>
#include <cstdint>

namespace plexus {

// The network-unique identity of a single publishing endpoint: the owning node's
// 16-byte node_id concatenated with a per-process endpoint counter. Deterministic
// and network-unique BY CONSTRUCTION — there is no hot-path RNG and no match-time
// exchange: the node_id half is pinned at handshake and the counter is minted at
// endpoint declaration, so a restarted process never re-emits a prior gid (the
// session epoch disambiguates a reused counter).
//
// A struct, NOT a type alias: node_id is a bare std::array alias that would
// implicitly convert, and the three identities (node_id / session_id /
// publisher_gid) must stay distinct so a developer cannot conflate them. The
// constituents are reached through node_id() / endpoint_counter() accessors;
// ordering and equality are the defaulted member-wise compare (node_id first,
// then the counter), which is unsigned-lexicographic over the node_id bytes.
struct publisher_gid
{
    plexus::node_id m_node_id{};
    std::uint64_t m_endpoint_counter{};

    const plexus::node_id &node_id() const noexcept
    {
        return m_node_id;
    }
    std::uint64_t endpoint_counter() const noexcept
    {
        return m_endpoint_counter;
    }

    auto operator<=>(const publisher_gid &) const = default;
};

}

#endif

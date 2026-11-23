#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_IDENTITY_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_IDENTITY_H

#include "plexus/io/endpoint_id.h"

#include "plexus/node_id.h"

#include <cstdint>
#include <string_view>

namespace plexus::detail {

// FNV-1a 64 over the name, run twice with distinct offset baselines, folding the two 8-byte
// digests into the 16-byte node_id. Deterministic and platform-stable: the same name yields the
// same identity.
inline node_id hash_node_id(std::string_view name) noexcept
{
    constexpr std::uint64_t k_prime    = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 14695981039346656037ull;
    // The high lane is a project lane with its own basis, not reference FNV-1a.
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t lo                   = k_basis_lo;
    std::uint64_t hi                   = k_basis_hi;
    for(char c : name)
    {
        const auto byte = static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        lo              = (lo ^ byte) * k_prime;
        hi              = (hi ^ byte) * k_prime;
    }
    return io::detail::fold_digest_lanes(lo, hi);
}

}

namespace plexus {

// Equal to the id() of a node constructed with the same name string.
inline node_id node_id_from_name(std::string_view name) noexcept
{
    return detail::hash_node_id(name);
}

}

#endif

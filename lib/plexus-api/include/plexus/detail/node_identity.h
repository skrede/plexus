#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_IDENTITY_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_IDENTITY_H

#include "plexus/node_id.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::detail {

// FNV-1a 64 over the name, run twice with distinct offset baselines, folding the two 8-byte
// digests into the 16-byte node_id. Deterministic and platform-stable: the same name yields the
// same identity.
inline node_id hash_node_id(std::string_view name) noexcept
{
    constexpr std::uint64_t k_prime    = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 1469598103934665603ull;
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t lo                   = k_basis_lo;
    std::uint64_t hi                   = k_basis_hi;
    for(char c : name)
    {
        const auto byte = static_cast<std::uint64_t>(static_cast<unsigned char>(c));
        lo              = (lo ^ byte) * k_prime;
        hi              = (hi ^ byte) * k_prime;
    }
    node_id id{};
    for(int i = 0; i < 8; ++i)
    {
        id[i]     = static_cast<std::byte>((lo >> (8 * i)) & 0xff);
        id[8 + i] = static_cast<std::byte>((hi >> (8 * i)) & 0xff);
    }
    return id;
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

#ifndef HPP_GUARD_PLEXUS_IO_ENDPOINT_ID_H
#define HPP_GUARD_PLEXUS_IO_ENDPOINT_ID_H

#include "plexus/io/endpoint.h"

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io {

// FNV-1a 64 over scheme, a separator byte, then address, run twice with distinct offset baselines
// and folded into the 16-byte node_id (the same scheme as the name digest). The separator keeps
// {"se","rial..."} from colliding with {"ser","ial..."}. Deterministic and stable per endpoint, so
// a redial to the same endpoint mints the same provisional id and dedups.
inline node_id endpoint_id(const endpoint &ep) noexcept
{
    constexpr std::uint64_t k_prime    = 1099511628211ull;
    constexpr std::uint64_t k_basis_lo = 1469598103934665603ull;
    constexpr std::uint64_t k_basis_hi = 0x9e3779b97f4a7c15ull;
    std::uint64_t lo                   = k_basis_lo;
    std::uint64_t hi                   = k_basis_hi;
    const auto mix                     = [&](std::uint64_t byte)
    {
        lo = (lo ^ byte) * k_prime;
        hi = (hi ^ byte) * k_prime;
    };
    for(char c : ep.scheme)
        mix(static_cast<unsigned char>(c));
    mix(0u);
    for(char c : ep.address)
        mix(static_cast<unsigned char>(c));
    node_id id{};
    for(int i = 0; i < 8; ++i)
    {
        id[i]     = static_cast<std::byte>((lo >> (8 * i)) & 0xff);
        id[8 + i] = static_cast<std::byte>((hi >> (8 * i)) & 0xff);
    }
    return id;
}

}

#endif

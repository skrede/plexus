#ifndef HPP_GUARD_PLEXUS_DISCOVERY_UNIVERSE_H
#define HPP_GUARD_PLEXUS_DISCOVERY_UNIVERSE_H

#include <string>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::discovery {

// Stable label -> universe id via 32-bit FNV-1a: deterministic for identical bytes on every
// platform (unlike std::hash, which is toolchain-defined) — the property two nodes agreeing on a
// universe require.
constexpr std::uint32_t universe_from_label(std::string_view label) noexcept
{
    std::uint32_t h = 2166136261u;
    for(char c : label)
    {
        h ^= static_cast<std::uint32_t>(static_cast<unsigned char>(c));
        h *= 16777619u;
    }
    return h;
}

// The authoritative default universe label. It is a concrete single-segment literal that intersects
// only itself — never a match-all wildcard — so a configless node partitions cleanly rather than
// rendezvousing with every universe.
inline constexpr std::string_view k_default_universe_label = "plexus.default";

// universe == 0 is the wire-uninitialized sentinel; configless nodes send this constant, not 0.
// The static_assert pins the hash cross-platform — a toolchain computing a different value fails
// to compile.
inline constexpr std::uint32_t k_default_universe = universe_from_label(k_default_universe_label);
static_assert(k_default_universe == 0x58E1B347u);

enum class universe_scoping : std::uint8_t
{
    soft, // all universes share the group; the inbound compare partitions
    hard  // per-universe derived group; kernel/IGMP filters, the compare stays as layer 2
};

// MurmurHash3 fmix32 finalizer (github.com/aappleby/smhasher): avalanches all bits to <0.25% bias.
constexpr std::uint32_t mix32(std::uint32_t h) noexcept
{
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

// 239.255.x.y with x in [1,254]: excludes 239.255.0.* (base group and DDS SPDP) and 239.255.255.*
// (SSDP, SLPv2) by range, not by special case.
constexpr std::pair<std::uint8_t, std::uint8_t> universe_group_octets(std::uint32_t universe) noexcept
{
    const std::uint32_t idx = mix32(universe) % (254u * 256u);
    return {static_cast<std::uint8_t>(1u + idx / 256u), static_cast<std::uint8_t>(idx % 256u)};
}

// Setup-time only (runs once at factory construction), never the hot path.
inline std::string universe_group(std::uint32_t universe)
{
    const auto [x, y] = universe_group_octets(universe);
    return "239.255." + std::to_string(x) + "." + std::to_string(y);
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_LOCALITY_H
#define HPP_GUARD_PLEXUS_IO_LOCALITY_H

#include <cstdint>
#include <string_view>

namespace plexus::io {

enum class locality : std::uint8_t
{
    process = 1u << 0,
    local   = 1u << 1,
    remote  = 1u << 2,
    any     = process | local | remote,
};

// operator~ masks back to `any`'s three bits: a stray high bit in a complement would
// make any_set spuriously match.
constexpr locality operator|(locality a, locality b) noexcept
{
    return static_cast<locality>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

constexpr locality operator&(locality a, locality b) noexcept
{
    return static_cast<locality>(static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b));
}

constexpr locality operator~(locality a) noexcept
{
    return static_cast<locality>(~static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(locality::any));
}

constexpr bool any_set(locality mask, locality tier) noexcept
{
    return static_cast<std::uint8_t>(mask & tier) != 0u;
}

// Fail-closed: an unrecognized scheme classifies remote, so a confined topic is
// never delivered over a transport we cannot prove is same-host.
inline locality tier_of(std::string_view scheme) noexcept
{
    if(scheme == "inproc")
        return locality::process;
    if(scheme == "unix")
        return locality::local;
    return locality::remote;
}

}

#endif

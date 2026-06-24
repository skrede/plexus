#ifndef HPP_GUARD_PLEXUS_IO_DISPATCH_HINT_H
#define HPP_GUARD_PLEXUS_IO_DISPATCH_HINT_H

#include <cstdint>

namespace plexus::io {

enum class dispatch_hint : std::uint8_t
{
    none     = 0,
    frequent = 1u << 0,
    large    = 1u << 1,
    priority = 1u << 2,
};

constexpr dispatch_hint operator|(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

constexpr dispatch_hint operator&(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b));
}

constexpr bool any_set(dispatch_hint h) noexcept
{
    return static_cast<std::uint8_t>(h) != 0u;
}

}

#endif

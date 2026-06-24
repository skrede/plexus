#ifndef HPP_GUARD_PLEXUS_IO_DISPATCH_HINT_H
#define HPP_GUARD_PLEXUS_IO_DISPATCH_HINT_H

#include <cstdint>

namespace plexus::io {

// A composable bitflag naming WHY a topic might prefer a faster delivery medium.
// frequent = a high-rate topic; large = a wide payload; priority = a
// latency-sensitive topic. The bits are powers of two so a mask unions any subset
// (a topic can be frequent AND large at once); none (0) is the ABSENCE of any hint.
// This is a medium-agnostic selection INPUT: the value is carried opaquely by the
// generic upgrade seam, and a medium's policy (or a future transport's) reads the
// bits to decide routing. It grants no cross-host reach and overrides no locality
// confinement.
//
// none = 0 is the absence, NOT an std::optional: a field defaulting to none means
// "no hint declared", and a declared-none is indistinguishable from an undeclared
// one, so a plain enum with a 0 absence is the correct shape (plexus has no QoS
// negotiation, so no "unset" sentinel is needed).
enum class dispatch_hint : std::uint8_t
{
    none     = 0,
    frequent = 1u << 0,
    large    = 1u << 1,
    priority = 1u << 2,
};

// Bitwise composition over the mask (the locality.h cast-based idiom). operator|
// unions co-occurring hints; operator& is the bit test.
constexpr dispatch_hint operator|(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

constexpr dispatch_hint operator&(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b));
}

// Any-bit-set: the generic predicate the default upgrade policy reads. none -> false.
constexpr bool any_set(dispatch_hint h) noexcept
{
    return static_cast<std::uint8_t>(h) != 0u;
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_LOCALITY_H
#define HPP_GUARD_PLEXUS_IO_LOCALITY_H

#include <cstdint>
#include <string_view>

namespace plexus::io {

// The delivery-tier confinement axis: a composable bitflag naming WHERE a topic's
// bytes may travel. process = same address space, local = same host (e.g. AF_UNIX),
// remote = off-host (TCP/TLS/UDP). The bits are powers of two so a mask unions any
// subset; `any` is all three (the default — no confinement). This is mechanism, not
// policy: the publisher declares a reach mask and the fan-out gate (and the engine's
// demand gate) refuse any tier the mask excludes, so a `local`-confined topic's bytes
// never leave the host even over an encrypted transport.
enum class locality : std::uint8_t
{
    process = 1u << 0,
    local   = 1u << 1,
    remote  = 1u << 2,
    any     = process | local | remote,
};

// Bitwise composition over the mask. The to-underlying helper is a C++23 facility;
// the C++20 floor uses static_cast<std::uint8_t>. operator~ masks back to `any`'s three bits so the
// complement of a mask never leaves a high bit set (a stray high bit would make
// any_set spuriously match).
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
    return static_cast<locality>(~static_cast<std::uint8_t>(a) &
                                 static_cast<std::uint8_t>(locality::any));
}

// The fan-out predicate: a mask delivers to a tier iff they share at least one bit.
// A `process`-only mask reaches no remote channel; `any` reaches every tier.
constexpr bool any_set(locality mask, locality tier) noexcept
{
    return static_cast<std::uint8_t>(mask & tier) != 0u;
}

// Classify a transport's endpoint scheme into its delivery tier. The scheme comes
// from the channel that minted the connection — never from peer-supplied data. An
// unrecognized scheme classifies remote (most-restrictive-to-leak, fail-closed): a
// confined topic is never delivered over a transport we cannot prove is same-host.
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

#ifndef HPP_GUARD_PLEXUS_IO_SHM_DISPATCH_HINT_H
#define HPP_GUARD_PLEXUS_IO_SHM_DISPATCH_HINT_H

#include <cstdint>

namespace plexus::io::shm {

// The publisher-or-subscriber dispatch hint: a composable bitflag naming WHY a
// topic might prefer the same-host shared-memory medium over the local stream.
// frequent = a high-rate topic (the per-message syscall of a stream dominates);
// large = a wide payload (the extra copy of a stream dominates); priority = a
// latency-sensitive topic. The bits are powers of two so a mask unions any subset
// (a topic can be frequent AND large at once); `none` (0) is the ABSENCE of any
// hint — the topic stays on the local stream (AF_UNIX). This is mechanism, not
// policy: a hint only gates whether a same-host side ATTEMPTS the ring acquire; it
// grants no cross-host reach and overrides no locality confinement.
//
// `none = 0` is deliberately the absence (D-01) — NOT an std::optional. A field
// defaulting to none means "no hint declared"; there is nothing to distinguish a
// declared-none from an undeclared one, so a plain enum with a 0 absence is the
// correct shape (plexus has no QoS negotiation, so no "unset" sentinel is needed).
//
// priority -> SHM is the weakest of the three motivations (a latency win from
// shared memory is real but small next to the frequent/large copy-and-syscall
// wins); it is flagged for empirical pruning at v0.1.5 (a sweep may show priority
// alone does not justify a ring). This is a tuning note, not a deferred feature —
// the bit ships and is honored today.
enum class dispatch_hint : std::uint8_t
{
    none     = 0,
    frequent = 1u << 0,
    large    = 1u << 1,
    priority = 1u << 2,
};

// Bitwise composition over the mask (the locality.h cast-based idiom). operator&
// is the bit test the predicate below reads; operator| unions co-occurring hints.
constexpr dispatch_hint operator|(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) | static_cast<std::uint8_t>(b));
}

constexpr dispatch_hint operator&(dispatch_hint a, dispatch_hint b) noexcept
{
    return static_cast<dispatch_hint>(static_cast<std::uint8_t>(a) & static_cast<std::uint8_t>(b));
}

// The SHM-eligibility predicate (D-02): a topic prefers the shared-memory medium
// iff ANY hint bit is set. none -> false (the topic stays on the local stream).
// This is the dispatch half of the selector decision; the locality (same-host)
// half is the other factor the transport_selector composes with it.
constexpr bool shm_eligible(dispatch_hint h) noexcept
{
    return static_cast<std::uint8_t>(h) != 0u;
}

}

#endif

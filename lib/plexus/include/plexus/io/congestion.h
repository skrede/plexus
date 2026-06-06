#ifndef HPP_GUARD_PLEXUS_IO_CONGESTION_H
#define HPP_GUARD_PLEXUS_IO_CONGESTION_H

#include <cstdint>

namespace plexus::io {

// The back-pressure axis: a single-valued choice naming WHAT a publisher does
// when the send path cannot accept more — `drop` sheds the new value at the
// publisher (the inherent best_effort behavior), `block` back-pressures the
// publish until the path drains (the safe default for a reliable topic, so a
// reliable guarantee is never silently violated by an overrun). Like
// reliability this is an exclusive choice, not a composable mask, so no
// bitwise operators.
enum class congestion : std::uint8_t
{
    drop,
    block,
};

}

#endif

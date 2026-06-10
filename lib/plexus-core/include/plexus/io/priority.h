#ifndef HPP_GUARD_PLEXUS_IO_PRIORITY_H
#define HPP_GUARD_PLEXUS_IO_PRIORITY_H

#include <cstdint>

namespace plexus::io {

// The egress-ordering axis: a single-valued LOCAL selector naming WHICH priority
// band a topic's published frames drain through under contention. It is strictly
// off-wire — it never rides byte_channel::send(span) nor any frame field; plexus
// is direct-delivery, so no relay needs it on the wire. A higher priority drains
// ahead of a lower one when a destination is back-pressured (the egress scheduler
// pops the highest non-empty band first); on an unloaded path there is no banding.
// Like reliability/congestion this is an exclusive choice, not a composable mask,
// so no bitwise operators. The default is `normal` so an undeclared topic lands in
// a middle band — a flood of undeclared traffic cannot starve a deliberately-high
// topic, and a deliberately-background topic still yields to undeclared traffic.
//
// The band COUNT is a fixed setup-time bound (see priority_band_queue.h); the value
// is to be substantiated empirically at the fan-out benchmark rather than fixed by
// feel — kept small so the per-destination band cost is a tiny constant.
enum class priority : std::uint8_t
{
    background = 0,
    normal     = 1,
    high       = 2,
    realtime   = 3,
};

}

#endif

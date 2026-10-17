#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_CHANNEL_TRAITS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_CHANNEL_TRAITS_H

#include "plexus/io/fragmentation.h"

#include <cstddef>
#include <cstdint>

namespace plexus::io::detail {

// The low-water bound for a banded channel that publishes no finite write-queue cap: one max-message, so the channel
// holds roughly one frame in flight and the bands hold the rest.
constexpr std::size_t k_low_water = io::fragmentation_limits::max_message_size;

// A channel that exposes its backpressure occupancy bands; one that does not is sent synchronously.
template<typename Channel>
constexpr bool can_poll()
{
    if constexpr(requires(Channel &c) { c.backpressured(); })
        return true;
    else
        return false;
}

// The per-construction scheduler key, so a reconnect at a reused heap address cannot collide with a freed channel's band
// entry. A channel without the verb never reaches the bands; the 0 branch only lets it instantiate.
template<typename Channel>
std::uint64_t key_of(Channel &ch)
{
    if constexpr(requires(Channel &c) { c.scheduler_key(); })
        return ch.scheduler_key();
    else
        return 0;
}

// The channel's own write-queue byte cap when it publishes one, so the band hand-off and the channel's admission stay in
// lockstep.
template<typename Channel>
std::size_t low_water_of(Channel &ch)
{
    if constexpr(requires(Channel &c) { c.write_queue_capacity(); })
        return ch.write_queue_capacity();
    else
        return k_low_water;
}

// A busy channel admits a frame while its occupancy plus the frame stays within the low-water bound; an idle one admits a
// frame of any size, since its own write-queue cap is the real per-frame bound and a large message must not strand in
// the bands.
template<typename Channel>
bool admits(Channel &ch, std::size_t size)
{
    if constexpr(can_poll<Channel>())
        return ch.backpressured() == 0 || ch.backpressured() + size <= low_water_of(ch);
    else
        return true;
}

}

#endif

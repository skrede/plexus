#ifndef HPP_GUARD_PLEXUS_IO_EGRESS_CAPACITY_H
#define HPP_GUARD_PLEXUS_IO_EGRESS_CAPACITY_H

#include "plexus/io/fragmentation.h"

#include <cstddef>

namespace plexus::io {

// The per-CONNECTION egress in-flight depth: the bounded byte budget of queued-but-unsent
// frames a congestion=block producer may accumulate BEHIND an in-flight message before it
// back-pressures (or sheds, under drop_newest). The depth half of the per-connection
// back-pressure QoS and the sibling of `congestion`: congestion names WHAT happens at the
// bound, egress_capacity names WHERE the bound sits. An empty queue always admits one
// within-ceiling message of any size — the per-message ceiling enforced at publish is the
// SOLE authority over message size; this cap only governs how much EXTRA backlog may pile up.
//
// bounded_default() (1x the max-message ceiling) is the latency-first / bounded-RSS posture:
// roughly one message in flight, so the deep priority-ordered backlog lives in the
// forwarder's egress bands ABOVE this shallow socket-facing FIFO. deep(multiple) trades RSS
// for throughput on a SATURATING single-topic stream (a back-to-back >~4 MiB firehose) where
// the one-message-in-flight rule otherwise halves the pipe. There is deliberately no
// `unbounded` factory: the QoS surface is always a finite byte cap (the egress scheduler's
// low-water gate moves in lockstep with it, so a deep cap is fed deeper and a shallow one is
// never over-fed). A LOCAL consumer choice, never remotely settable; per-connection, never
// per-topic (a deep per-topic FIFO on a shared connection re-accumulates an un-prioritized
// backlog and defeats banding).
struct egress_capacity
{
    std::size_t bytes;

    [[nodiscard]] static constexpr egress_capacity bounded_default() noexcept
    {
        return {1u * fragmentation_limits::max_message_size};
    }

    [[nodiscard]] static constexpr egress_capacity deep(std::size_t multiple) noexcept
    {
        return {multiple * fragmentation_limits::max_message_size};
    }

    [[nodiscard]] static constexpr egress_capacity of_bytes(std::size_t n) noexcept
    {
        return {n};
    }
};

}

#endif

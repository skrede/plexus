#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_SCHEDULER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_SCHEDULER_H

#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/wire_bytes.h"

#include "plexus/policy.h"

#include <span>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace plexus::io::detail {

// The low-water gate FALLBACK: the scheduler keeps feeding a destination only while its
// queued byte occupancy is below the gate — the priority-ordered backlog must live in the
// bands, NOT in the channel's FIFO. The gate tracks the channel's OWN write-queue byte cap
// (see low_water_of) so the band hand-off and the channel's admission stay in lockstep; this
// constant is only the bound for a banded channel that publishes no finite cap, sized at one
// max-message so the channel holds roughly one frame in flight and the bands hold the rest.
constexpr std::size_t k_low_water = io::fragmentation_limits::max_message_size;

// The forwarder-owned, per-destination priority-band egress scheduler. It sits BETWEEN
// the publish fan-out loop and channel.send(): publish enqueues a framed buffer into the
// destination's bands, then a drain pops the highest non-empty band and sends it while
// the destination can still accept, holding the rest of the backlog priority-ordered in
// the bands so a higher-priority topic leaves a contended destination before a flooding
// lower-priority one.
//
// A channel that exposes backpressured() (a stream channel) bands; one that does not
// (inproc/sink) SHORT-CIRCUITS to a direct synchronous send — byte-identical to the
// pre-scheduler path, no banding, no post, no reorder. The capability is read via
// if constexpr on the concrete Channel, so NO byte_channel concept verb is added.
//
// The drain runs INLINE at enqueue while the destination accepts; the steady-state path
// allocates nothing (the bands and pooled buffers are grown once at setup). When the
// destination goes full the remaining backlog stays priority-ordered in the bands and the
// NEXT publish's enqueue resumes the drain (event-driven re-arm). A periodic liveness
// re-poll for a destination that goes full AND then receives no further publishes is a
// separate later concern.
//
// The per-message congestion policy (block / drop_oldest / drop_newest) is applied at the
// destination band when it saturates: block and drop_newest refuse the new frame,
// drop_oldest evicts the oldest resident frame and admits the new one. Each outcome is
// observable through a per-(destination, band) counter (dropped_oldest / dropped_newest /
// blocked) mirroring the channel-level dropped_count() shape.
//
// In-flight safety: a band node stays pool-resident until channel.send() has taken the
// frame — sharing the owner into its own send queue (the wire_bytes overload, no copy) or
// copying it (the span-only TLS/inproc fallback); only THEN is pop_highest called, so the
// scheduler never frees a node the socket is mid-writing — drop_oldest only ever recycles a
// slot still resident in a band, never one already handed to channel.send().
template<typename Channel, typename Policy>
    requires plexus::Policy<Policy>
class egress_scheduler
{
public:
    // Enqueue a framed buffer for a destination at the given band under the publisher's
    // per-message congestion policy. A channel without a backpressure signal is sent
    // synchronously (the inproc/sink short-circuit) — it has no bounded band backlog, so
    // there is no saturation site and congestion does not apply on the passthrough; a
    // stream channel bands the frame (applying congestion at a full band) then drains the
    // highest-priority backlog the destination can currently accept.
    // Returns the drop cause the admission incurred (drop_cause::none on a clean admit) so
    // the forwarder records the per-(topic, band) drop at the fan-out site. The inproc/sink
    // short-circuit never has a bounded band backlog, so it never drops here (none).
    drop_cause enqueue(Channel &ch, std::size_t band, io::congestion congestion, wire_bytes<> frame)
    {
        if constexpr(!can_poll())
        {
            // inproc/sink: no bounded band backlog, so congestion is moot. The owner
            // converts to a span for the direct send — byte-identical to the pre-owner
            // passthrough (the short-circuit never holds a backlog to pin).
            ch.send(static_cast<std::span<const std::byte>>(frame));
            return drop_cause::none;
        }
        else
        {
            const drop_cause cause = m_queues[key_of(ch)].enqueue_with_verdict(band, congestion, std::move(frame));
            drain(ch);
            return cause;
        }
    }

    // Drop a departed peer's bands (peer-death cleanup, called beside the registry removal).
    void remove(Channel &ch)
    {
        m_queues.erase(key_of(ch));
    }

    // The per-(destination, band) overflow counters, mirroring asio_channel::dropped_count():
    // each reads the destination's band counter, returning 0 for a destination with no queue.
    std::size_t dropped_oldest(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(key_of(ch));
        return it == m_queues.end() ? 0u : it->second.dropped_oldest_count(band);
    }

    std::size_t dropped_newest(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(key_of(ch));
        return it == m_queues.end() ? 0u : it->second.dropped_newest_count(band);
    }

    std::size_t blocked(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(key_of(ch));
        return it == m_queues.end() ? 0u : it->second.blocked_count(band);
    }

private:
    // True when the Channel exposes a backpressure occupancy read — the banding gate.
    static constexpr bool can_poll()
    {
        if constexpr(requires(Channel &c) { c.backpressured(); })
            return true;
        else
            return false;
    }

    // The stable map key for a channel: its per-construction scheduler_key() (so a reconnect
    // at a reused heap address cannot collide with a freed channel's band entry), read via a
    // capability probe mirroring can_poll(). A channel without the verb is the inproc/sink
    // short-circuit that never reaches m_queues, so its key is never the banding path — the
    // 0 branch only exists so the no-verb case still instantiates.
    static std::uint64_t key_of(Channel &ch)
    {
        if constexpr(requires(Channel &c) { c.scheduler_key(); })
            return ch.scheduler_key();
        else
            return 0;
    }

    // Pop the highest non-empty band and send it while the channel has headroom for THAT
    // frame's bytes; leave any remaining backlog priority-ordered in the bands for the next
    // publish to resume (event-driven re-arm — strict highest-band-first, low-band
    // starvation is intended). The gate is headroom-for-the-frame, NOT bare occupancy: the
    // low-water threshold equals the channel's own write-queue byte cap, so popping a frame
    // the channel would refuse at that cap would lose it AND bypass the band drop counters.
    // Checking backpressured()+size() against the gate keeps the band and channel bounds in
    // agreement — the band never hands over a frame the channel cannot admit (channel
    // SIGNALS occupancy, the band DECIDES the hand-off).
    void drain(Channel &ch)
    {
        auto &q = m_queues[key_of(ch)];
        while(q.has_work())
        {
            const auto *node = q.front_highest();
            if(!admits(ch, node->size()))
                break;
            // Hand the owner to the channel BEFORE the band advances: a channel with a
            // wire_bytes send overload shares the owner into its send queue (no copy);
            // one with only the span overload copies (the TLS/inproc fallback). Either
            // way the bytes are pinned by the channel before pop_highest releases the slot.
            ch.send(*node);
            q.pop_highest();
        }
    }

    // The channel has room for a frame of this size while its queued occupancy plus the
    // frame stays within the low-water gate; a channel with no backpressure signal always
    // admits (the short-circuit path). The low-water gate keeps the priority-ordered backlog
    // in the bands (not the channel FIFO) under contention, but an IDLE channel admits a
    // single frame of ANY size — the channel's own write-queue cap is the real per-frame
    // bound — so a large single message (above the low-water band gate) is not stranded in
    // the bands forever. Without the idle short-circuit a message larger than the low-water
    // gate could never hand off, capping the message-size envelope at the gate.
    bool admits(Channel &ch, std::size_t size) const
    {
        if constexpr(can_poll())
            return ch.backpressured() == 0 || ch.backpressured() + size <= low_water_of(ch);
        else
            return true;
    }

    // The low-water bound for THIS channel: its own write-queue byte cap when it publishes one
    // (so the band hand-off and the channel's admission stay in lockstep — a deepened cap is fed
    // deeper, a shallow one is never over-fed past what the channel will accept), else the shared
    // fallback for a banded channel that exposes occupancy but no finite cap.
    static std::size_t low_water_of(Channel &ch)
    {
        if constexpr(requires(Channel &c) { c.write_queue_capacity(); })
            return ch.write_queue_capacity();
        else
            return k_low_water;
    }

    std::unordered_map<std::uint64_t, priority_band_queue> m_queues;
};

}

#endif

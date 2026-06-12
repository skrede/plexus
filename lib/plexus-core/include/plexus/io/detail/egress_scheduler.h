#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_SCHEDULER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_EGRESS_SCHEDULER_H

#include "plexus/io/congestion.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/priority_band_queue.h"

#include "plexus/policy.h"

#include <span>
#include <cstddef>
#include <unordered_map>

namespace plexus::io::detail {

// The low-water gate: the scheduler keeps feeding a destination only while its queued
// byte occupancy is below this. It is the load-bearing knob — the priority-ordered
// backlog must live in the bands, NOT in the channel's FIFO, so the gate is set to one
// max-message so the channel holds roughly one frame in flight and the bands hold the
// rest. The value is to be substantiated at the fan-out benchmark, not fixed by feel.
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
// separate later concern; the borrowed executor is threaded in now as the substrate that
// re-poll will post onto — the scheduler holds no owned runtime, no thread, no
// shared_from_this, and the engine quiesces the executor before teardown.
//
// The per-message congestion policy (block / drop_oldest / drop_newest) is applied at the
// destination band when it saturates: block and drop_newest refuse the new frame,
// drop_oldest evicts the oldest resident frame and admits the new one. Each outcome is
// observable through a per-(destination, band) counter (dropped_oldest / dropped_newest /
// blocked) mirroring the channel-level dropped_count() shape.
//
// In-flight safety: a band node stays pool-resident until channel.send() COPIES it into
// the channel's own send queue; only THEN is pop_highest called, so the scheduler never
// frees a node the socket is mid-writing — drop_oldest only ever recycles a slot still
// resident in a band, never one already handed to channel.send().
template <typename Channel, typename Policy>
    requires plexus::Policy<Policy>
class egress_scheduler
{
public:
    using executor_type = typename Policy::executor_type;

    explicit egress_scheduler(executor_type executor)
        : m_executor(executor)
    {
    }

    // Enqueue a framed buffer for a destination at the given band under the publisher's
    // per-message congestion policy. A channel without a backpressure signal is sent
    // synchronously (the inproc/sink short-circuit) — it has no bounded band backlog, so
    // there is no saturation site and congestion does not apply on the passthrough; a
    // stream channel bands the frame (applying congestion at a full band) then drains the
    // highest-priority backlog the destination can currently accept.
    // Returns the drop cause the admission incurred (drop_cause::none on a clean admit) so
    // the forwarder records the per-(topic, band) drop at the fan-out site. The inproc/sink
    // short-circuit never has a bounded band backlog, so it never drops here (none).
    drop_cause enqueue(Channel &ch, std::size_t band, io::congestion congestion,
                       std::span<const std::byte> frame)
    {
        if constexpr(!can_poll())
        {
            ch.send(frame);   // inproc/sink: no bounded band backlog, so congestion is moot
            return drop_cause::none;
        }
        else
        {
            const drop_cause cause = m_queues[&ch].enqueue_with_verdict(band, congestion, frame);
            drain(ch);
            return cause;
        }
    }

    // Drop a departed peer's bands (peer-death cleanup, called beside the registry removal).
    void remove(Channel &ch)
    {
        m_queues.erase(&ch);
    }

    // The per-(destination, band) overflow counters, mirroring asio_channel::dropped_count():
    // each reads the destination's band counter, returning 0 for a destination with no queue.
    [[nodiscard]] std::size_t dropped_oldest(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(&ch);
        return it == m_queues.end() ? 0u : it->second.dropped_oldest_count(band);
    }

    [[nodiscard]] std::size_t dropped_newest(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(&ch);
        return it == m_queues.end() ? 0u : it->second.dropped_newest_count(band);
    }

    [[nodiscard]] std::size_t blocked(Channel &ch, std::size_t band) const
    {
        const auto it = m_queues.find(&ch);
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

    // Pop the highest non-empty band and send it while the channel accepts; leave any
    // remaining backlog priority-ordered in the bands for the next publish to resume
    // (event-driven re-arm — strict highest-band-first, low-band starvation is intended).
    void drain(Channel &ch)
    {
        auto &q = m_queues[&ch];
        while(accepts(ch) && q.has_work())
        {
            const auto *node = q.front_highest();
            ch.send(*node);   // copies into the channel's send queue BEFORE the band advances
            q.pop_highest();
        }
    }

    // The channel can take more while its queued occupancy is below the low-water gate;
    // a channel with no backpressure signal always accepts (the short-circuit path).
    bool accepts(Channel &ch) const
    {
        if constexpr(can_poll())
            return ch.backpressured() < k_low_water;
        else
            return true;
    }

    executor_type m_executor;
    std::unordered_map<Channel *, priority_band_queue> m_queues;
};

}

#endif

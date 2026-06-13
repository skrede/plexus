#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PRIORITY_BAND_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PRIORITY_BAND_QUEUE_H

#include "plexus/io/priority.h"
#include "plexus/io/congestion.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/wire_bytes.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>
#include <algorithm>

namespace plexus::io::detail {

// The fixed number of egress priority bands one destination is split into. Kept a
// small constant so the per-destination band cost is bounded; the exact count is to
// be substantiated at the fan-out benchmark, not fixed by feel.
constexpr std::size_t k_egress_bands = 4;

// The fixed per-band message capacity (a setup-time bound). The per-destination
// memory ceiling is k_egress_bands * k_band_depth * max_payload; the depth is to be
// swept at the fan-out benchmark rather than fixed by feel.
constexpr std::size_t k_band_depth = 256;

// Map a priority to a band index: realtime -> 0 (highest drain priority) ...
// background -> 3. The drain order is ascending band index = descending priority, so
// the inversion subtracts the enum value from the top band; the result is clamped to
// the last band for a hypothetical out-of-range value.
inline std::size_t band_of(priority p) noexcept
{
    const std::size_t inverted = k_egress_bands - 1 - static_cast<std::size_t>(p);
    return std::min(inverted, k_egress_bands - 1);
}

// One destination's N priority bands of frame-owner handles. Each band is a
// fixed-depth FIFO ring whose slots are grown ONCE (cloning the history_ring
// grow-once / assign-into-slot pattern) and reused thereafter, so a steady
// enqueue/pop loop allocates nothing of its own. A slot holds a wire_bytes owner
// addref-shared from the forwarder's frame-once-fan-to-N buffer (one frame alloc per
// publish, shared across destinations — never a per-slot byte copy), and PINS that
// owner resident until drain hands it to channel.send. A band at capacity applies the
// per-message congestion policy: block and drop_newest REFUSE the new frame (resident
// set untouched, a counter bumped), drop_oldest EVICTS the oldest resident slot
// (advance the FIFO head, releasing that one owner) and admits the new frame into the
// freed tail, so the resident count stays k_band_depth.
class priority_band_queue
{
public:
    // True when any band holds a frame.
    [[nodiscard]] bool has_work() const noexcept
    {
        for(const auto &b : m_bands)
            if(!b.empty())
                return true;
        return false;
    }

    // Admit a framed-buffer owner into the given band under the per-message congestion
    // mode (sharing the owner — addref, no byte copy). A non-full band admits regardless
    // of mode; a full band applies the policy: block and drop_newest refuse (return
    // false), drop_oldest evicts the oldest and admits (return true).
    bool enqueue(std::size_t band, io::congestion congestion, wire_bytes<> frame)
    {
        return m_bands[band].push(congestion, std::move(frame));
    }

    // As enqueue, but returns the drop cause that the admission incurred (drop_cause::none
    // on a clean admit; drop_oldest when a full band evicted its oldest to admit; drop_newest
    // / blocked when a full band refused the new frame). The per-(topic, band) drop counter
    // is bumped from this verdict at the forwarder fan-out site.
    drop_cause enqueue_with_verdict(std::size_t band, io::congestion congestion, wire_bytes<> frame)
    {
        return m_bands[band].push_with_verdict(congestion, std::move(frame));
    }

    // The front owner of the highest non-empty band (lowest index), or nullptr when no
    // band holds work. The owner stays band-resident (pinning the bytes) until the caller
    // hands it to channel.send and then calls pop_highest — the in-flight-safe ordering.
    [[nodiscard]] const wire_bytes<> *front_highest() const
    {
        for(const auto &b : m_bands)
            if(!b.empty())
                return &b.front();
        return nullptr;
    }

    // Advance the FIFO tail of the highest non-empty band (the same band front_highest
    // returned). A no-op when every band is empty.
    void pop_highest()
    {
        for(auto &b : m_bands)
            if(!b.empty())
                return b.pop();
    }

    [[nodiscard]] std::size_t dropped_oldest_count(std::size_t band) const noexcept
    {
        return m_bands[band].dropped_oldest();
    }

    [[nodiscard]] std::size_t dropped_newest_count(std::size_t band) const noexcept
    {
        return m_bands[band].dropped_newest();
    }

    [[nodiscard]] std::size_t blocked_count(std::size_t band) const noexcept
    {
        return m_bands[band].blocked();
    }

private:
    // A single band: a fixed-depth FIFO ring of frame-owner slots grown once and
    // reused via move-assign, with a per-message congestion admission at capacity.
    struct band
    {
        [[nodiscard]] bool empty() const noexcept { return m_count == 0; }

        // Admit into the slot one-past the tail, growing the ring to k_band_depth on
        // first touch. At capacity the congestion mode decides: block/drop_newest
        // refuse, drop_oldest evicts the front to make room (handled by the helper).
        // True iff the new frame was admitted (a refusing verdict — drop_newest/blocked —
        // is the only non-admit). Expressed over push_with_verdict so the two cannot diverge.
        bool push(io::congestion congestion, wire_bytes<> frame)
        {
            const drop_cause cause = push_with_verdict(congestion, std::move(frame));
            return cause != drop_cause::drop_newest && cause != drop_cause::blocked;
        }

        // The verdict-returning admission: drop_cause::none on a clean admit, drop_oldest
        // when a full band evicted to admit, drop_newest/blocked when a full band refused.
        drop_cause push_with_verdict(io::congestion congestion, wire_bytes<> frame)
        {
            drop_cause cause = drop_cause::none;
            if(m_count == k_band_depth)
            {
                cause = evict_oldest_or_refuse_cause(congestion);
                if(cause == drop_cause::drop_newest || cause == drop_cause::blocked)
                    return cause;   // refused: the resident set is untouched
            }
            if(m_slots.size() != k_band_depth)
                m_slots.resize(k_band_depth);
            const std::size_t tail = (m_head + m_count) % k_band_depth;
            m_slots[tail] = std::move(frame);
            ++m_count;
            return cause;   // none on a clean admit, drop_oldest when an eviction made room
        }

        // At a full band: block/drop_newest bump their counter and refuse (returning the
        // refusing cause); drop_oldest recycles the oldest resident slot (pop() — never a
        // free), bumps its counter and returns drop_oldest so the caller admits the new
        // frame into the freed tail. The cause mirrors the per-cause counter it bumps.
        drop_cause evict_oldest_or_refuse_cause(io::congestion congestion)
        {
            switch(congestion)
            {
            case io::congestion::block:       ++m_blocked;        return drop_cause::blocked;
            case io::congestion::drop_newest: ++m_dropped_newest; return drop_cause::drop_newest;
            case io::congestion::drop_oldest: pop(); ++m_dropped_oldest; return drop_cause::drop_oldest;
            }
            return drop_cause::blocked;
        }

        [[nodiscard]] const wire_bytes<> &front() const { return m_slots[m_head]; }

        // Advance the FIFO head and release the popped slot's owner (the bytes are now
        // pinned by the channel's send queue), so the band holds no stale reference.
        void pop() noexcept
        {
            m_slots[m_head] = wire_bytes<>{};
            m_head = (m_head + 1) % k_band_depth;
            --m_count;
        }

        [[nodiscard]] std::size_t dropped_oldest() const noexcept { return m_dropped_oldest; }
        [[nodiscard]] std::size_t dropped_newest() const noexcept { return m_dropped_newest; }
        [[nodiscard]] std::size_t blocked() const noexcept { return m_blocked; }

        std::vector<wire_bytes<>> m_slots;
        std::size_t m_head{0};
        std::size_t m_count{0};
        std::size_t m_dropped_oldest{0};
        std::size_t m_dropped_newest{0};
        std::size_t m_blocked{0};
    };

    std::array<band, k_egress_bands> m_bands;
};

}

#endif

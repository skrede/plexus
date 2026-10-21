#ifndef HPP_GUARD_PLEXUS_SHM_SLOT_PUBLISHER_H
#define HPP_GUARD_PLEXUS_SHM_SLOT_PUBLISHER_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <thread>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::shm {

// The producer over one broadcast ring: it derives a fixed delivery policy from the
// QoS once at construction (so the hot loan() never re-reads it), then exposes the
// two-step loan -> fill -> publish handle protocol the channel facade wraps. reliable
// gates claim_with_policy on the slowest registered cursor; best_effort overwrites the
// latest, skipping pinned slots (the cross-transport drop_oldest analog).
class slot_publisher
{
public:
    slot_publisher(broadcast_ring &ring, io::reliability rel, io::congestion cong) noexcept
            : m_ring(ring)
            , m_reliability(rel)
            , m_congestion(cong)
    {
    }

    slot_publisher(const slot_publisher &)            = delete;
    slot_publisher &operator=(const slot_publisher &) = delete;
    slot_publisher(slot_publisher &&)                 = delete;
    slot_publisher &operator=(slot_publisher &&)      = delete;

    loan_status loan(std::size_t size, loaned_buffer &out) noexcept
    {
        broadcast_ring::claim_result claim;
        const loan_status st = m_ring.claim_with_policy(size, m_reliability, m_congestion, claim);
        if(st != loan_status::ok)
            return st;

        out = loaned_buffer(m_ring, claim.slab, claim.position);
        return st;
    }

    // The ring's commit handles the memory-ordering edge. A commit the ring rejects leaves the handle engaged, so its
    // destruction abandons the position rather than leaving the cell unadvanced.
    loan_status publish(loaned_buffer &&buffer) noexcept
    {
        loaned_buffer spent         = std::move(buffer);
        const loan_status committed = m_ring.commit(spent.m_position, spent.m_filled);
        if(committed == loan_status::ok)
            spent.disengage();
        return committed;
    }

    io::reliability delivery() const noexcept
    {
        return m_reliability;
    }
    io::congestion overflow() const noexcept
    {
        return m_congestion;
    }

    // The back-pressure progress signal a blocking reliable producer watches to tell a
    // live-but-slow consumer from a wedged one.
    std::uint64_t slowest_consumer_position() const noexcept
    {
        return m_ring.slowest_consumer_position();
    }

    loan_status loan_until_stalled(std::size_t size, loaned_buffer &out) noexcept
    {
        loan_status st          = loan(size, out);
        std::uint64_t last_seen = slowest_consumer_position();
        for(std::int32_t stalled = 0; st == loan_status::congested && stalled < k_no_progress_budget; ++stalled)
        {
            std::this_thread::yield();
            st = loan(size, out);

            const std::uint64_t now = slowest_consumer_position();
            if(now != last_seen)
            {
                last_seen = now;
                stalled   = -1; // a live consumer freed a slot: reset the window (++ returns it to 0)
            }
        }
        return st;
    }

private:
    // The cap on CONSECUTIVE no-progress yield turns before a still-congested reliable+block
    // loan surfaces congested. Gated on consumer PROGRESS, not a fixed turn count: every time
    // the slowest cursor advances the consumer freed a slot, so blocking stays LOSSLESS for any
    // live-but-lagging consumer; the budget only elapses across a whole window of NO cursor
    // motion (a wedged/dead peer), surfacing congested observably rather than hanging.
    static constexpr std::int32_t k_no_progress_budget = 1 << 16;

    broadcast_ring &m_ring;
    io::reliability m_reliability;
    io::congestion m_congestion;
};

}

#endif

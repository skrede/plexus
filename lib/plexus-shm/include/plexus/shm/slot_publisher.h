#ifndef HPP_GUARD_PLEXUS_SHM_SLOT_PUBLISHER_H
#define HPP_GUARD_PLEXUS_SHM_SLOT_PUBLISHER_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <cstddef>
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

        out = loaned_buffer(claim.slab.data(), claim.slab.size(), claim.position);
        return st;
    }

    // The ring's commit handles the memory-ordering edge. Consumes the handle by move.
    loan_status publish(loaned_buffer &&buffer) noexcept
    {
        loaned_buffer spent = std::move(buffer);
        return m_ring.commit(spent.m_position, spent.m_filled);
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

private:
    broadcast_ring &m_ring;
    io::reliability m_reliability;
    io::congestion m_congestion;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_SHM_SLOT_PUBLISHER_H
#define HPP_GUARD_PLEXUS_IO_SHM_SLOT_PUBLISHER_H

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loaned_buffer.h"
#include "plexus/io/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <cstddef>
#include <utility>

namespace plexus::io::shm {

// The producer endpoint over one broadcast ring: it derives a fixed delivery
// policy from the topic's QoS ONCE at construction (the reliable/best_effort
// class and the block/drop_newest congestion mode become two stored fields, so the hot
// loan() never re-reads the QoS), then exposes the two-step loan -> fill ->
// publish handle protocol the channel facade wraps.
//
//   loan(size, out)         claim a slot for a `size`-byte message under the
//                           construction-derived policy and hand back a move-only
//                           loaned_buffer window the caller fills. rejected when
//                           size exceeds the slot capacity (the oversize fallback
//                           surfaces HERE); congested when the policy's
//                           gate blocks (reliable: the slowest cursor has not
//                           drained; best_effort: a full lap is pinned).
//   publish(loaned_buffer)  commit the filled window by move; the buffer's
//                           recorded filled length becomes the descriptor payload
//                           length. Consumes the handle (it is spent after).
//
// The policy is derived from plexus::io::reliability + plexus::io::congestion
// (Shared Pattern E -- NO parallel SHM backpressure enum): reliable threads
// claim_with_policy(reliable) so the producer gates on the slowest registered
// cursor; best_effort threads claim_with_policy(best_effort) so it overwrites the
// latest, skipping pinned slots — the cross-transport drop_oldest analog. The
// congestion field rides through to the ring (block vs the drop modes is the
// publisher's contract; the ring surfaces congested either way and the
// channel/caller decides).
//
// Borrows the ring BY REFERENCE; non-copy/non-move owning endpoint.
class slot_publisher
{
public:
    slot_publisher(broadcast_ring &ring, reliability rel, congestion cong) noexcept
        : m_ring(ring), m_reliability(rel), m_congestion(cong)
    {
    }

    slot_publisher(const slot_publisher &) = delete;
    slot_publisher &operator=(const slot_publisher &) = delete;
    slot_publisher(slot_publisher &&) = delete;
    slot_publisher &operator=(slot_publisher &&) = delete;

    // Claims a slot for `size` bytes under the construction-derived policy. ok
    // hands back a writable window through `out`; rejected when size exceeds the
    // slot capacity (oversize); congested when the policy gate blocks.
    loan_status loan(std::size_t size, loaned_buffer &out) noexcept
    {
        broadcast_ring::claim_result claim;
        const loan_status st = m_ring.claim_with_policy(size, m_reliability, m_congestion, claim);
        if(st != loan_status::ok)
            return st;

        out = loaned_buffer(claim.slab.data(), claim.slab.size(),
                            claim.position & (m_ring.cell_count() - 1), claim.position);
        return st;
    }

    // Commits the filled window: writes the recorded filled length as the
    // descriptor payload length and releases the cell's sequence (the ring's
    // commit handles the memory-ordering edge). Consumes the handle by move.
    loan_status publish(loaned_buffer &&buffer) noexcept
    {
        loaned_buffer spent = std::move(buffer);
        return m_ring.commit(spent.m_position, spent.m_filled);
    }

    reliability delivery() const noexcept { return m_reliability; }
    congestion overflow() const noexcept { return m_congestion; }

private:
    broadcast_ring &m_ring;
    reliability     m_reliability;
    congestion      m_congestion;
};

}

#endif

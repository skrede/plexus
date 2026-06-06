#ifndef HPP_GUARD_PLEXUS_IO_SHM_SLOT_SUBSCRIBER_H
#define HPP_GUARD_PLEXUS_IO_SHM_SLOT_SUBSCRIBER_H

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/taken_message.h"

#include <cstdint>

namespace plexus::io::shm {

// The consumer endpoint over one broadcast ring. It owns its OWN in-region read
// cursor: it registers a cursor at construction (starting at the producer's tail
// so it sees only messages published after it joins) and unregisters it at
// destruction (so the cursor stops gating producer reclamation). A registration
// failure (the ring is at its k_max_consumers bound) leaves the endpoint with no
// cursor; every take() then no-ops to empty.
//
//   take(out)   read the next message for this cursor. ok hands back a move-only
//               taken_message aliasing the slot bytes, the slot pinned for the
//               handle's lifetime, and advances the cursor by one; empty when
//               there is nothing new (the would-block case); a lap-behind or a
//               best_effort skip-tombstone steps the cursor forward and retries
//               within the call, so the caller only ever sees ok/empty.
//
// The pin is Dekker-safe: take() pins via the ring's pin_if_current (the seq_cst
// announce + recheck that rules out a best_effort overwrite stomping the read),
// and hands that ALREADY-HELD pin to the taken_message via its adopt_pin ctor --
// it does NOT pin a second time (the carry-forward "must not double-pin" rule).
//
// Borrows the ring BY REFERENCE; non-copy/non-move owning endpoint.
class slot_subscriber
{
public:
    explicit slot_subscriber(broadcast_ring &ring) noexcept
        : m_ring(ring)
    {
        if(m_ring.register_cursor(m_cursor_index) == loan_status::ok)
        {
            m_registered = true;
            m_cursor     = m_ring.tail_position();
            m_ring.publish_cursor(m_cursor_index, m_cursor);
        }
    }

    ~slot_subscriber()
    {
        if(m_registered)
            m_ring.unregister_cursor(m_cursor_index);
    }

    slot_subscriber(const slot_subscriber &) = delete;
    slot_subscriber &operator=(const slot_subscriber &) = delete;
    slot_subscriber(slot_subscriber &&) = delete;
    slot_subscriber &operator=(slot_subscriber &&) = delete;

    // Reads the next message for this cursor, resolving lap-behind/skip-tombstone
    // internally so the caller only ever observes ok or empty.
    loan_status take(taken_message &out) noexcept
    {
        if(!m_registered)
            return loan_status::empty;

        for(;;)
        {
            broadcast_ring::consume_result consumed;
            const loan_status st = m_ring.consume(m_cursor, consumed);
            if(st == loan_status::empty)
                return loan_status::empty;
            if(st == loan_status::congested)
            {
                advance(); // a lap-behind cursor or a skip tombstone: step forward
                continue;
            }

            // ok: a deliverable slot. Pin it Dekker-safe BEFORE advancing. If the
            // pin lost the overwrite race (a best_effort producer recycled the slot
            // between consume and pin), step forward and retry rather than alias a
            // torn read.
            if(!m_ring.pin_if_current(m_cursor))
            {
                advance();
                continue;
            }

            out = taken_message(taken_message::adopt_pin, consumed.slab.data(),
                                consumed.slab.size(), &m_ring.refcount_at(m_cursor),
                                m_cursor & m_ring.cell_count() - 1, m_cursor);
            advance();
            return loan_status::ok;
        }
    }

    bool registered() const noexcept { return m_registered; }
    std::uint64_t cursor() const noexcept { return m_cursor; }

private:
    void advance() noexcept
    {
        ++m_cursor;
        m_ring.publish_cursor(m_cursor_index, m_cursor);
    }

    broadcast_ring &m_ring;
    std::uint32_t   m_cursor_index{0};
    std::uint64_t   m_cursor{0};
    bool            m_registered{false};
};

}

#endif

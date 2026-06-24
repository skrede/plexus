#ifndef HPP_GUARD_PLEXUS_SHM_SLOT_SUBSCRIBER_H
#define HPP_GUARD_PLEXUS_SHM_SLOT_SUBSCRIBER_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/cpu_relax.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/taken_message.h"

#include <cstdint>

namespace plexus::shm {

// The consumer io::endpoint over one broadcast ring. It owns its OWN in-region read
// cursor: it registers a cursor at construction (starting at the producer's tail
// so it sees only messages published after it joins) and unregisters it at
// destruction (so the cursor stops gating producer reclamation). A registration
// failure (the ring is at its k_max_consumers bound) leaves the io::endpoint with no
// cursor; every take() then no-ops to empty.
//
//   take(out)   read the next message for this cursor. ok hands back a move-only
//               taken_message aliasing the slot bytes, the slot pinned for the
//               handle's lifetime, and advances the cursor by one; empty when
//               there is nothing new (the would-block case); a small-contention
//               lap or a best_effort skip-tombstone steps the cursor forward, and
//               a full-ring lap-behind jumps the cursor to the producer tail in
//               one step (O(1)) -- all retried within the call, so the caller only
//               ever sees ok/empty.
//
// The pin is Dekker-safe: take() pins via the ring's pin_if_current (the seq_cst
// announce + recheck that rules out a best_effort overwrite stomping the read),
// and hands that ALREADY-HELD pin to the taken_message via its adopt_pin ctor --
// it does NOT pin a second time (the carry-forward "must not double-pin" rule).
//
// Adaptive spin-then-park (consumer-sovereign): take() spins up to spin_budget empty
// reads before reporting empty, so a back-to-back message landing within the budget is
// caught at near-busy-poll latency without falling through to the notifier's futex park;
// a genuinely-idle consumer exhausts the budget, returns empty, and the notifier parks at
// ~0% CPU. The budget is a required-WITH-default consumer policy knob — 0 = always park
// (the futex floor), large = effectively always spin (busy-poll). The park MECHANISM stays
// the backend notifier's futex/io_uring wait; this is only the GENERIC consumer spin
// policy, so it lives here in core. The default is the conservative swept knee (see
// default_spin_budget): it catches a genuine back-to-back arrival while staying
// CPU-cheap and park-dominated, with the knob for a latency-maximalist consumer to opt up.
//
// Borrows the ring BY REFERENCE; non-copy/non-move owning io::endpoint.
class slot_subscriber
{
public:
    // The conservative default spin budget, confirmed by a {0,64,256,1k,4k,16k} x rate x
    // payload sweep on the bench rig (shm-spin-budget-sweep-2026-06-13): the latency knee is
    // rate-dependent (the spin window only catches the next message when arrival falls inside
    // it), so no fixed budget is optimal everywhere. 256 reclaims a real share of the wakeup
    // cost on the high-rate back-to-back path over pure-park (~-50% P50 at 100kHz) at a sub-1us
    // per-message spin window, never busy-spins idle (0% at 1Hz), and parks otherwise; a
    // latency-maximalist consumer raises this knob rather than the default imposing a large
    // per-message spin burn on every consumer.
    static constexpr std::uint32_t default_spin_budget = 256;

    explicit slot_subscriber(broadcast_ring &ring, std::uint32_t spin_budget = default_spin_budget) noexcept
            : m_ring(ring)
            , m_spin_budget(spin_budget)
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

    slot_subscriber(const slot_subscriber &)            = delete;
    slot_subscriber &operator=(const slot_subscriber &) = delete;
    slot_subscriber(slot_subscriber &&)                 = delete;
    slot_subscriber &operator=(slot_subscriber &&)      = delete;

    // Reads the next message for this cursor, resolving lap-behind/skip-tombstone
    // internally so the caller only ever observes ok or empty.
    loan_status take(taken_message &out) noexcept
    {
        if(!m_registered)
            return loan_status::empty;

        std::uint32_t spun = 0;
        for(;;)
        {
            broadcast_ring::consume_result consumed;
            const loan_status              st = m_ring.consume(m_cursor, consumed);
            if(st == loan_status::empty)
            {
                if(spin_or_give_up(spun))
                    return loan_status::empty;
                continue;
            }
            loan_status settled;
            if(resolve(st, consumed, out, settled))
                return settled;
        }
    }

    bool registered() const noexcept
    {
        return m_registered;
    }
    std::uint64_t cursor() const noexcept
    {
        return m_cursor;
    }

private:
    // Adaptive spin-then-park: a back-to-back message may land within the budget, so spin
    // (relaxing the core) and retry rather than reporting empty immediately. Returns true past
    // the budget so the caller reports empty and the backend futex park takes over (~0% CPU idle).
    [[nodiscard]] bool spin_or_give_up(std::uint32_t &spun) noexcept
    {
        if(spun++ >= m_spin_budget)
            return true;
        cpu_relax();
        return false;
    }

    // Resolve a non-empty consume: lagged jumps the cursor to the surfaced producer tail in one
    // O(1) step; congested (small-contention dif>0 or a skip tombstone) steps forward; ok pins the
    // slot Dekker-safe before advancing, retrying on a lost overwrite race rather than aliasing a
    // torn read. Returns true (settled = ok) only on a delivered slot; false means retry the loop.
    bool resolve(loan_status st, const broadcast_ring::consume_result &consumed, taken_message &out, loan_status &settled) noexcept
    {
        if(st == loan_status::lagged)
        {
            m_cursor = consumed.position;
            m_ring.publish_cursor(m_cursor_index, m_cursor);
            return false;
        }
        if(st == loan_status::congested || !m_ring.pin_if_current(m_cursor))
        {
            advance();
            return false;
        }
        out = taken_message(taken_message::adopt_pin, consumed.slab.data(), consumed.slab.size(), &m_ring.refcount_at(m_cursor));
        advance();
        settled = loan_status::ok;
        return true;
    }

    void advance() noexcept
    {
        ++m_cursor;
        m_ring.publish_cursor(m_cursor_index, m_cursor);
    }

    broadcast_ring &m_ring;
    std::uint32_t   m_spin_budget;
    std::uint32_t   m_cursor_index{0};
    std::uint64_t   m_cursor{0};
    bool            m_registered{false};
};

}

#endif

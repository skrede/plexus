#ifndef HPP_GUARD_PLEXUS_SHM_RING_CLAIM_H
#define HPP_GUARD_PLEXUS_SHM_RING_CLAIM_H

#include "plexus/shm/cpu_relax.h"
#include "plexus/shm/ring_cells.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_cursors.h"

#include "plexus/io/reliability.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace plexus::shm::detail {

// PRODUCER half of the overwrite-vs-pin Dekker announce on a slot already won. Stores the claiming marker on `sequence`
// (seq_cst) then seq_cst-loads take_refcount; the consumer's seq_cst pin + seq_cst recheck pair rules out
// store-buffering. Returns true when no live take pins the slot.
inline bool announce_and_check_pin(cell_t &c, std::uint64_t position) noexcept
{
    c.sequence.store(position, std::memory_order_seq_cst); // claiming marker
    return c.take_refcount.load(std::memory_order_seq_cst) == 0;
}

// Stamp the SKIPPED tombstone on the cell a best-effort producer donates past: advance its sequence to (pos + 1) so a
// future claim recognizes it via the Vyukov dif, but mark payload_len so consume() treats it as non-deliverable.
inline void stamp_skip(cell_t &c, std::uint64_t position) noexcept
{
    c.payload_len.store(k_skip_len, std::memory_order_relaxed);
    c.sequence.store(position + 1, std::memory_order_release);
}

// True when the slot serving `position` holds a COMMITTED prior occupant (or is fresh), so a claim may recycle it
// without stealing a claim in flight. dif==0 is fresh first-lap; dif==1-cell_count is filled-and-lapped (stale bytes);
// dif==-cell_count is a claim in flight.
inline bool occupant_committed(const cell_t &c, std::uint64_t position, std::uint64_t cell_count) noexcept
{
    const std::uint64_t seq = c.sequence.load(std::memory_order_acquire);
    const std::int64_t dif  = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(position);
    return dif == 0 || dif == 1 - static_cast<std::int64_t>(cell_count);
}

// Waits out the transient take pin on a freshly-won reliable position, returning ok the moment no live pin
// remains. The no-progress budget RESETS whenever the slowest consumer cursor advances, so a live-but-slow pin
// holder blocks losslessly however slow; only a wedged holder (no cursor motion across the whole window) trips it,
// and then the position is converted to a skip-tombstone (NOT abandoned) so contiguity holds and the bail is
// OBSERVABLE as congested rather than a silent FIFO hole.
template<typename Ring>
loan_status clear_pin_or_skip(Ring &r, std::uint64_t pos, typename Ring::claim_result &out) noexcept
{
    std::uint64_t last_seen = r.slowest_consumer_position();
    for(std::uint32_t stalled = 0; !announce_and_check_pin(r.cell_at(pos), pos); ++stalled)
    {
        const std::uint64_t now = r.slowest_consumer_position();
        if(now != last_seen)
        {
            last_seen = now;
            stalled   = 0; // the pin holder is alive: keep waiting losslessly
        }
        else if(stalled >= Ring::k_pin_clear_spin_budget)
        {
            stamp_skip(r.cell_at(pos), pos);
            return loan_status::congested;
        }
        cpu_relax();
    }
    out.position = pos;
    out.slab     = r.slot_span(pos);
    return loan_status::ok;
}

// Gates BEFORE winning the position so reliable never burns a slot it cannot fill: the slowest cursor must have drained
// the prior occupant, and that occupant must be committed, since with no cursor nothing else keeps a lap off a claim in
// flight (a stale `pos` reloads instead). A consumer may still pin the won position's aliased read, so the claim is
// announced and the pin waited out (Disruptor gate: reliable keeps FIFO contiguity, the pin is transient); a won
// position resolves to ok or to a recognized skip-tombstone, never abandoned, so no consumer wedges on it.
template<typename Ring>
loan_status claim_reliable(Ring &r, std::uint64_t &pos, typename Ring::claim_result &out) noexcept
{
    for(;;)
    {
        if(!consumers_passed(*r.m_header, r.m_cell_count, pos))
            return loan_status::congested;
        if(!occupant_committed(r.cell_at(pos), pos, r.m_cell_count))
        {
            const std::uint64_t now = r.m_header->enqueue_pos.load(std::memory_order_relaxed);
            if(now == pos)
                return loan_status::congested;
            pos = now;
            continue;
        }
        if(!r.m_header->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            continue;
        return clear_pin_or_skip(r, pos, out);
    }
}

// Follows enqueue progress until the slot serving `pos` holds a committed occupant (or is fresh); false once
// k_pin_clear_spin_budget relax turns pass with neither, since a claim in flight one lap behind may be one this producer
// still holds or a dead producer left, and those never commit.
template<typename Ring>
bool await_committed_occupant(Ring &r, std::uint64_t &pos) noexcept
{
    for(std::uint32_t stalled = 0; !occupant_committed(r.cell_at(pos), pos, r.m_cell_count);)
    {
        const std::uint64_t now = r.m_header->enqueue_pos.load(std::memory_order_relaxed);
        stalled                 = now == pos ? stalled + 1 : 0;
        if(stalled >= Ring::k_pin_clear_spin_budget)
            return false;
        pos = now;
        cpu_relax();
    }
    return true;
}

// Recycles only a slot whose prior occupant is committed, giving up congested on one that stays in flight. A won but
// pinned position is donated past with a recognizable tombstone and the next one tried; a full lap of pinned cells is
// the bounded congested fallback (at most k_max_consumers pins).
template<typename Ring>
loan_status claim_best_effort(Ring &r, std::uint64_t &pos, typename Ring::claim_result &out) noexcept
{
    for(std::uint64_t skipped = 0;;)
    {
        if(!await_committed_occupant(r, pos))
            return loan_status::congested;
        if(!r.m_header->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
            continue;
        if(announce_and_check_pin(r.cell_at(pos), pos))
        {
            out.position = pos;
            out.slab     = r.slot_span(pos);
            return loan_status::ok;
        }
        stamp_skip(r.cell_at(pos), pos);
        if(++skipped >= r.m_cell_count)
            return loan_status::congested;
        pos = r.m_header->enqueue_pos.load(std::memory_order_relaxed);
    }
}

// reliable -> congested when the slowest registered cursor has not passed the cell (and spins out a transient pin);
// best-effort -> overwrite-latest, skipping pinned cells, congested only when a full lap is pinned.
template<typename Ring>
loan_status claim_with_policy(Ring &r, std::size_t size, io::reliability rel, typename Ring::claim_result &out) noexcept
{
    if(size > r.slot_capacity())
        return loan_status::rejected;

    std::uint64_t pos = r.header().enqueue_pos.load(std::memory_order_relaxed);
    if(rel == io::reliability::reliable)
        return claim_reliable(r, pos, out);
    return claim_best_effort(r, pos, out);
}

// Writes the descriptor fields relaxed, then releases the sequence so an acquiring consumer observes the payload
// writes; filled_len > slot_capacity -> rejected.
template<typename Ring>
loan_status ring_commit(Ring &r, std::uint64_t position, std::size_t filled_len) noexcept
{
    if(filled_len > r.m_slot_capacity)
        return loan_status::rejected;

    cell_t &c = r.cell_at(position);
    c.payload_len.store(static_cast<std::uint32_t>(filled_len), std::memory_order_relaxed);
    c.sequence.store(position + 1, std::memory_order_release);
    return loan_status::ok;
}

// Turns a won position that will never be committed into the skip tombstone every consumer steps past and the next lap
// recycles, so a dropped claim cannot stop the ring at its cell.
template<typename Ring>
void ring_abandon(Ring &r, std::uint64_t position) noexcept
{
    stamp_skip(r.cell_at(position), position);
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_SHM_RING_CONSUME_H
#define HPP_GUARD_PLEXUS_SHM_RING_CONSUME_H

#include "plexus/shm/ring_cells.h"
#include "plexus/shm/loan_status.h"

#include <atomic>
#include <cstddef>
#include <cstdint>

namespace plexus::shm::detail {

// READER side of the overwrite-vs-pin Dekker handshake. Pins the cell at `cursor` (seq_cst fetch_add on
// take_refcount) then RE-LOADS its sequence seq_cst: if it still reads cursor+1 the pin landed before any best-effort
// overwrite could win the slot (the matching seq_cst announce/recheck pair rules out store-buffering), so the pin holds
// -- returns true. Otherwise the pin is dropped and false returned so the caller skips a torn read.
inline bool pin_if_current(cell_t &c, std::uint64_t cursor) noexcept
{
    c.take_refcount.fetch_add(1, std::memory_order_seq_cst); // announce the pin
    if(c.sequence.load(std::memory_order_seq_cst) == cursor + 1)
        return true;                                         // the slot is still our committed message: the pin holds
    c.take_refcount.fetch_sub(1, std::memory_order_seq_cst); // overwritten: unpin
    return false;
}

// sequence==cursor+1 -> ok with a read-only slab span; nothing newer -> empty; lapped by at least a full ring ->
// lagged, carrying the producer tail so the caller jumps there in one step; the producer mid-commit less than a lap
// ahead, or a skip tombstone -> congested (the caller steps its cursor forward by one without delivering).
template<typename Ring>
loan_status ring_consume(Ring &r, std::uint64_t cursor, typename Ring::consume_result &out) noexcept
{
    cell_t &c               = r.cell_at(cursor);
    const std::uint64_t seq = c.sequence.load(std::memory_order_acquire);
    const std::int64_t dif  = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(cursor + 1);
    if(dif < 0)
        return loan_status::empty;
    if(dif > 0)
    {
        if(dif >= static_cast<std::int64_t>(r.m_cell_count))
        {
            out.position = r.tail_position();
            return loan_status::lagged;
        }
        return loan_status::congested;
    }

    const std::uint32_t len = c.payload_len.load(std::memory_order_relaxed);
    if(len == k_skip_len)
        return loan_status::congested;

    out.position = cursor;
    out.slab     = r.slot_span(cursor).subspan(0, static_cast<std::size_t>(len > r.m_slot_capacity ? r.m_slot_capacity : len));
    return loan_status::ok;
}

}

#endif

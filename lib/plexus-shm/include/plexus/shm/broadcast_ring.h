#ifndef HPP_GUARD_PLEXUS_SHM_BROADCAST_RING_H
#define HPP_GUARD_PLEXUS_SHM_BROADCAST_RING_H

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/cpu_relax.h"
#include "plexus/shm/loan_status.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>
#include <utility>

namespace plexus::shm {

// over-limit: one Vyukov-sequenced lock-free ring; the claim/commit/consume cursor protocol
// + the seq_cst overwrite-vs-pin Dekker handshake are one indivisible whole over the shared
// header/cells/cursor atomics — the in-class index accessors only bind member state to the
// layout (already in ring_layout.h) and pulling them out threads m_cells/m_slab/m_mask/
// m_stride through every hot path, scattering the protocol without separating a responsibility.
//
// Lock-free, cross-process, offset-based MPMC broadcast ring: the Vyukov bounded MPMC queue
// adapted to DESCRIPTOR cells (each fixed cache-line cell carries a slab byte offset + length
// + the per-cell sequence; the variable-size payload lives in a separate fixed-stride slab
// keyed 1:1 to the cells). It exposes the low-level claim/commit/consume mechanism the
// loan/publish/take endpoints wrap, plus the broadcast overlay (per-consumer in-region
// cursors) and the two backpressure modes io::reliability/io::congestion select. Header-only (the
// atomics + offset math inline); it drives a caller-supplied control+cells span and a payload
// slab span — the caller owns the mapping lifetime.
//
// BROADCAST OVERLAY (the Disruptor gating idea over Vyukov): every subscriber must observe
// every committed slot, so each owns its own read cursor in the header's fixed-bound cursor
// array. The producer reclamation gate is "the slowest registered consumer has passed this
// slot"; a sentinel (k_cursor_idle) cursor does not gate.
//
// BACKPRESSURE: claim_with_policy threads the publisher's delivery class — reliable blocks a
// cell until take_refcount==0 AND the slowest non-sentinel cursor is strictly past it (the
// publisher does a bounded spin+backoff, no kernel object); best-effort overwrites latest but
// SKIPS any pinned cell, with a full-lap-pinned congested fallback. take_refcount is acq_rel;
// the reclamation-gating cursor loads/stores are acquire/release.
//
// CRASH STANCE (honest — crash-SAFE, NOT crash-LIVE): no cross-process lock is held, so a
// dying process never corrupts the ring; but a producer that dies after the enqueue CAS
// before the sequence release leaves a never-advancing cell, and a consumer that dies holding
// a take() leaves take_refcount>0. Detect-and-reclaim is a DEFERRED hardening item.
//
// Non-copy/non-move owning service.
class broadcast_ring
{
public:
    // Sentinel cursor position meaning "registered but not yet gating": a fresh
    // cursor does not hold back reclamation until it reads its first slot.
    static constexpr std::uint64_t k_cursor_idle = UINT64_MAX;

    // No-progress bound on the reliable pin-clear spin: a reliable claim that has
    // WON a position but finds it still pinned by a live take() relaxes the core,
    // watching the slowest consumer cursor. The take pin is transient (the read
    // releases it), and a consumer that is draining advances its cursor -- so the
    // budget counts CONSECUTIVE relax turns during which the slowest cursor did NOT
    // move and RESETS on every advance: a live-but-slow consumer keeps the producer
    // blocking losslessly however slow it drains, while a genuinely wedged/dead pin
    // holder (no cursor motion across a whole window) trips the budget. The unpinned
    // happy path takes the budget zero times and is byte-identical. The window is the
    // same back-to-back order the consumer empty-retry budget covers (the swept knee,
    // shm-spin-budget-sweep-2026-06-14), here as the no-progress reset granularity.
    static constexpr std::uint32_t k_pin_clear_spin_budget = 256;

    // Result of a successful claim: the cell position (carries the lap) and a
    // writable, 8-aligned slab span the caller fills before committing.
    struct claim_result
    {
        std::uint64_t        position{0};
        std::span<std::byte> slab;
    };

    // Result of a successful consume: the read-only slab span for the message
    // and the cell position it was read from.
    struct consume_result
    {
        std::uint64_t              position{0};
        std::span<const std::byte> slab;
    };

    broadcast_ring() = default;

    broadcast_ring(const broadcast_ring &)            = delete;
    broadcast_ring &operator=(const broadcast_ring &) = delete;
    broadcast_ring(broadcast_ring &&)                 = delete;
    broadcast_ring &operator=(broadcast_ring &&)      = delete;

    // Places the control header + cells over `control` and initializes every
    // cell's sequence to its index per the Vyukov init. cell_count must be a
    // power of two; slot_capacity > 0. The spans must already be sized for the
    // geometry (control_region_bytes / slab_region_bytes).
    // NOLINTNEXTLINE(readability-function-size)
    static loan_status create(std::span<std::byte> control, std::span<std::byte> slab, std::uint64_t cell_count, std::uint64_t slot_capacity, broadcast_ring &out,
                              std::uint64_t consumer_capacity = k_max_consumers) noexcept
    {
        if(!is_power_of_two(cell_count) || slot_capacity == 0)
            return loan_status::rejected;
        if(consumer_capacity == 0 || consumer_capacity > k_max_consumers)
            return loan_status::rejected;
        if(control.size() < control_region_bytes(cell_count) || slab.size() < slab_region_bytes(cell_count, slot_capacity))
            return loan_status::rejected;

        auto *header = ::new(control.data()) control_header_t{};
        header->enqueue_pos.store(0, std::memory_order_relaxed);
        header->magic             = k_ring_magic;
        header->cell_count        = cell_count;
        header->slot_capacity     = slot_capacity;
        header->mask              = cell_count - 1;
        header->consumer_capacity = consumer_capacity;
        header->notify_generation.store(0, std::memory_order_relaxed);
        header->park_state.store(k_park_empty, std::memory_order_relaxed);
        header->high_water.store(0, std::memory_order_relaxed);
        for(std::size_t i = 0; i < k_max_consumers; ++i)
        {
            header->cursors[i].position.store(k_cursor_idle, std::memory_order_relaxed);
            header->cursors[i].active.store(0, std::memory_order_relaxed);
        }

        auto *cells = ::new(control.data() + sizeof(control_header_t)) cell_t[cell_count];
        for(std::uint64_t i = 0; i < cell_count; ++i)
        {
            cells[i].sequence.store(i, std::memory_order_relaxed);
            cells[i].payload_offset.store(0, std::memory_order_relaxed);
            cells[i].payload_len.store(0, std::memory_order_relaxed);
            cells[i].take_refcount.store(0, std::memory_order_relaxed);
        }

        out.bind(control, slab, header, cells, cell_count, slot_capacity, cell_count - 1, consumer_capacity);
        return loan_status::ok;
    }

    // Attaches over already-mapped spans and re-reads + bounds-checks the config
    // from the control header (never trusts a peer's separate value): magic +
    // version, power-of-two cell_count, mask == cell_count - 1, and both region
    // sizes. A foreign or layout-incompatible region is rejected.
    static loan_status attach(std::span<std::byte> control, std::span<std::byte> slab, broadcast_ring &out) noexcept
    {
        if(control.size() < sizeof(control_header_t))
            return loan_status::rejected;

        auto *header = std::launder(reinterpret_cast<control_header_t *>(control.data()));
        if(header->magic != k_ring_magic || !is_power_of_two(header->cell_count) || header->slot_capacity == 0)
            return loan_status::rejected;
        if(header->mask != header->cell_count - 1)
            return loan_status::rejected;
        if(header->consumer_capacity == 0 || header->consumer_capacity > k_max_consumers)
            return loan_status::rejected;

        const std::uint64_t stride = round_up_8(header->slot_capacity);
        if(control.size() < control_region_bytes(header->cell_count) || slab.size() < header->cell_count * stride)
            return loan_status::rejected;

        auto *cells = std::launder(reinterpret_cast<cell_t *>(control.data() + sizeof(control_header_t)));
        out.bind(control, slab, header, cells, header->cell_count, header->slot_capacity, header->mask, header->consumer_capacity);
        return loan_status::ok;
    }

    // Claims a free cell for a message of `size` bytes under the reliable policy
    // with no consumer gating. size>slot_capacity -> rejected; a free cell won ->
    // ok; ring full -> congested.
    // NOLINTNEXTLINE(readability-function-size)
    loan_status claim(std::size_t size, claim_result &out) noexcept
    {
        if(size > m_slot_capacity)
            return loan_status::rejected;

        std::uint64_t pos = m_header->enqueue_pos.load(std::memory_order_relaxed);
        for(;;)
        {
            cell_t             &c   = cell_at(pos);
            const std::uint64_t seq = c.sequence.load(std::memory_order_acquire);
            const std::int64_t  dif = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(pos);
            if(dif == 0)
            {
                if(m_header->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                    break;
            }
            else if(dif < 0)
                return loan_status::congested;
            else
                pos = m_header->enqueue_pos.load(std::memory_order_relaxed);
        }

        out.position = pos;
        out.slab     = slot_span(pos);
        return loan_status::ok;
    }

    // Claims a free cell honoring the backpressure policy and the consumer gate.
    // reliable -> congested when the slowest registered cursor has not passed the
    // cell (and spins out a transient pin); best-effort -> overwrite-latest,
    // skipping pinned cells, congested only when a full lap is pinned.
    loan_status claim_with_policy(std::size_t size, io::reliability rel, io::congestion, claim_result &out) noexcept
    {
        if(size > m_slot_capacity)
            return loan_status::rejected;

        std::uint64_t pos = m_header->enqueue_pos.load(std::memory_order_relaxed);
        if(rel == io::reliability::reliable)
            return claim_reliable(pos, out);
        return claim_best_effort(pos, out);
    }

    // Commits a claimed cell: writes the descriptor fields relaxed, then releases
    // the sequence so an acquiring consumer observes the payload writes.
    // filled_len > slot_capacity -> rejected.
    loan_status commit(std::uint64_t position, std::size_t filled_len) noexcept
    {
        if(filled_len > m_slot_capacity)
            return loan_status::rejected;

        cell_t &c = cell_at(position);
        c.payload_offset.store(static_cast<std::uint64_t>(position & m_mask) * m_stride, std::memory_order_relaxed);
        c.payload_len.store(static_cast<std::uint32_t>(filled_len), std::memory_order_relaxed);
        c.sequence.store(position + 1, std::memory_order_release);
        return loan_status::ok;
    }

    // Reads the cell at `cursor` for this consumer: sequence==cursor+1 -> ok with
    // a read-only slab span; nothing newer -> empty; the producer lapped this
    // cursor by at least a full ring (dif >= cell_count) -> lagged, carrying the
    // producer tail so the caller jumps there in one step; a small dif>0 (the
    // producer is mid-commit ahead, less than a lap) OR a skip tombstone ->
    // congested (the caller steps its cursor forward by one without delivering).
    loan_status consume(std::uint64_t cursor, consume_result &out) noexcept
    {
        cell_t             &c   = cell_at(cursor);
        const std::uint64_t seq = c.sequence.load(std::memory_order_acquire);
        const std::int64_t  dif = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(cursor + 1);
        if(dif < 0)
            return loan_status::empty;
        if(dif > 0)
        {
            if(dif >= static_cast<std::int64_t>(m_cell_count))
            {
                out.position = tail_position();
                return loan_status::lagged;
            }
            return loan_status::congested;
        }

        const std::uint32_t len = c.payload_len.load(std::memory_order_relaxed);
        if(len == k_skip_len)
            return loan_status::congested;

        out.position = cursor;
        out.slab     = const_slot_span(cursor).subspan(0, static_cast<std::size_t>(len > m_slot_capacity ? m_slot_capacity : len));
        return loan_status::ok;
    }

    // Registers a broadcast cursor in the header's fixed-bound array; out_index is
    // initialized to k_cursor_idle (not gating). rejected when the per-ring
    // consumer_capacity slots are already registered (<= k_max_consumers).
    loan_status register_cursor(std::uint32_t &out_index) noexcept
    {
        const std::uint32_t capacity = static_cast<std::uint32_t>(m_consumer_capacity);
        for(std::uint32_t i = 0; i < capacity; ++i)
        {
            std::uint32_t expected = 0;
            if(m_header->cursors[i].active.compare_exchange_strong(expected, 1, std::memory_order_acq_rel))
            {
                m_header->cursors[i].position.store(k_cursor_idle, std::memory_order_release);
                raise_high_water(i + 1);
                out_index = i;
                return loan_status::ok;
            }
        }
        return loan_status::rejected;
    }

    // Releases a previously-registered cursor so it stops gating reclamation.
    void unregister_cursor(std::uint32_t index) noexcept
    {
        if(index >= m_consumer_capacity)
            return;
        m_header->cursors[index].position.store(k_cursor_idle, std::memory_order_release);
        m_header->cursors[index].active.store(0, std::memory_order_release);
    }

    // Publishes this consumer's cursor position with release so the producer's
    // reclamation gate observes it.
    void publish_cursor(std::uint32_t index, std::uint64_t position) noexcept
    {
        if(index >= m_consumer_capacity)
            return;
        m_header->cursors[index].position.store(position, std::memory_order_release);
    }

    // The take_refcount atomic pinning the cell at `position`.
    std::atomic<std::uint32_t> &refcount_at(std::uint64_t position) noexcept
    {
        return cell_at(position).take_refcount;
    }

    // READER side of the overwrite-vs-pin Dekker handshake. Pins the cell at
    // `cursor` (seq_cst fetch_add on take_refcount) then RE-LOADS its sequence
    // seq_cst: if it still reads cursor+1 the pin landed before any best-effort
    // overwrite could win the slot (the matching seq_cst announce/recheck pair
    // rules out store-buffering), so the pin holds -- returns true. Otherwise the
    // pin is dropped and false returned so the caller skips a torn read.
    bool pin_if_current(std::uint64_t cursor) noexcept
    {
        cell_t &c = cell_at(cursor);
        c.take_refcount.fetch_add(1, std::memory_order_seq_cst); // announce the pin
        if(c.sequence.load(std::memory_order_seq_cst) == cursor + 1)
            return true;                                         // the slot is still our committed message: the pin holds
        c.take_refcount.fetch_sub(1, std::memory_order_seq_cst); // overwritten: unpin
        return false;
    }

    // The producer's current enqueue position -- a late-joining subscriber starts
    // its cursor here so it sees messages published after it registers.
    std::uint64_t tail_position() const noexcept
    {
        return m_header->enqueue_pos.load(std::memory_order_acquire);
    }

    // The slowest registered (non-idle) consumer cursor position, or k_cursor_idle
    // when none is gating. It is the producer back-pressure progress signal: this
    // value advances exactly when the lagging consumer that holds the reliable gate
    // closed drains a slot, so a blocking producer can tell a live-but-slow consumer
    // (the position moved) from a wedged/dead one (it did not).
    std::uint64_t slowest_consumer_position() const noexcept
    {
        std::uint64_t       slowest = k_cursor_idle;
        const std::uint32_t scan    = m_header->high_water.load(std::memory_order_acquire);
        for(std::uint32_t i = 0; i < scan; ++i)
        {
            if(m_header->cursors[i].active.load(std::memory_order_acquire) == 0)
                continue;
            const std::uint64_t cur = m_header->cursors[i].position.load(std::memory_order_acquire);
            if(cur != k_cursor_idle && cur < slowest)
                slowest = cur;
        }
        return slowest;
    }

    // The shared wakeup-generation word in this ring's control header (in mapped
    // shared memory). A producer in ANY process attached to this ring bumps it and
    // wakes a consumer blocked on it by address; the consumer's notifier
    // FUTEX_WAITs on this same word.
    std::atomic<std::uint32_t> &notify_generation() noexcept
    {
        return m_header->notify_generation;
    }

    // The parked-waiter 3-state word in this ring's control header (in mapped
    // shared memory). A consumer's notifier park boundary toggles it
    // EMPTY/PARKED/NOTIFIED across processes; the producer's gated wake reads it to
    // skip the FUTEX_WAKE syscall when no waiter is parked. Mirrors
    // notify_generation()'s cross-process word accessor.
    std::atomic<std::uint32_t> &park_state() noexcept
    {
        return m_header->park_state;
    }

    std::uint64_t cell_count() const noexcept
    {
        return m_cell_count;
    }
    std::uint64_t slot_capacity() const noexcept
    {
        return m_slot_capacity;
    }

    // The monotonic high-water of registered cursors (the largest index+1 ever
    // registered) that bounds the reliable-reclaim scan length.
    std::uint32_t registered_high_water() const noexcept
    {
        return m_header->high_water.load(std::memory_order_acquire);
    }

private:
    static bool is_power_of_two(std::uint64_t v) noexcept
    {
        return v != 0 && (v & (v - 1)) == 0;
    }

    void bind(std::span<std::byte> control, std::span<std::byte> slab, control_header_t *header, cell_t *cells, std::uint64_t cell_count, std::uint64_t slot_capacity,
              std::uint64_t mask, std::uint64_t consumer_capacity) noexcept
    {
        m_control           = control;
        m_slab              = slab;
        m_header            = header;
        m_cells             = cells;
        m_cell_count        = cell_count;
        m_slot_capacity     = slot_capacity;
        m_mask              = mask;
        m_stride            = round_up_8(slot_capacity);
        m_consumer_capacity = consumer_capacity;
    }

    cell_t &cell_at(std::uint64_t position) noexcept
    {
        return m_cells[position & m_mask];
    }

    // Monotonically raise the registered-consumer high-water to at least `want`.
    // A fresh cursor joins at tail_position(), so it cannot gate a reclaimable
    // occupant until a full ring of enqueues has elapsed -- ample release/acquire
    // edges for this release bump to reach the producer's acquire-bounded scan
    // before the cursor can hold back reclamation.
    void raise_high_water(std::uint32_t want) noexcept
    {
        std::uint32_t hw = m_header->high_water.load(std::memory_order_relaxed);
        while(hw < want && !m_header->high_water.compare_exchange_weak(hw, want, std::memory_order_release, std::memory_order_relaxed))
            ;
    }

    std::span<std::byte> slot_span(std::uint64_t position) noexcept
    {
        const std::size_t offset = static_cast<std::size_t>(position & m_mask) * m_stride;
        return m_slab.subspan(offset, m_stride);
    }

    std::span<const std::byte> const_slot_span(std::uint64_t position) const noexcept
    {
        const std::size_t offset = static_cast<std::size_t>(position & m_mask) * m_stride;
        return std::span<const std::byte>(m_slab).subspan(offset, m_stride);
    }

    // True when every registered, non-idle cursor has consumed the occupant the
    // claim at `position` would overwrite (the one at position - cell_count).
    bool consumers_passed(std::uint64_t position) const noexcept
    {
        if(position < m_cell_count)
            return true; // first lap: no prior occupant to free
        const std::uint64_t occupant = position - m_cell_count;
        const std::uint32_t scan     = m_header->high_water.load(std::memory_order_acquire);
        for(std::uint32_t i = 0; i < scan; ++i)
        {
            if(m_header->cursors[i].active.load(std::memory_order_acquire) == 0)
                continue;
            const std::uint64_t cur = m_header->cursors[i].position.load(std::memory_order_acquire);
            if(cur != k_cursor_idle && cur <= occupant)
                return false; // a registered consumer has not consumed the occupant yet
        }
        return true;
    }

    // True when the slot serving `position` holds a COMMITTED prior occupant (or
    // is fresh), so a best-effort overwrite may recycle it without stealing an
    // in-flight peer's claim. dif==0 is fresh first-lap; dif==1-cell_count is
    // filled-and-lapped (stale bytes); dif==-cell_count is an in-flight peer.
    bool occupant_committed(std::uint64_t position) const noexcept
    {
        const std::uint64_t seq = m_cells[position & m_mask].sequence.load(std::memory_order_acquire);
        const std::int64_t  dif = static_cast<std::int64_t>(seq) - static_cast<std::int64_t>(position);
        return dif == 0 || dif == 1 - static_cast<std::int64_t>(m_cell_count);
    }

    // PRODUCER half of the overwrite-vs-pin Dekker announce on a slot already won.
    // Stores the claiming marker on `sequence` (seq_cst) then seq_cst-loads
    // take_refcount; the consumer's seq_cst pin + seq_cst recheck pair rules out
    // store-buffering. Returns true when no live take pins the slot.
    bool announce_and_check_pin(std::uint64_t position) noexcept
    {
        cell_t &c = cell_at(position);
        c.sequence.store(position, std::memory_order_seq_cst); // claiming marker
        return c.take_refcount.load(std::memory_order_seq_cst) == 0;
    }

    // Stamp the SKIPPED tombstone on the cell a best-effort producer donates past:
    // advance its sequence to (pos + 1) so a future claim recognizes it via the
    // Vyukov dif, but mark payload_len so consume() treats it as non-deliverable.
    void stamp_skip(std::uint64_t position) noexcept
    {
        cell_t &c = cell_at(position);
        c.payload_len.store(k_skip_len, std::memory_order_relaxed);
        c.sequence.store(position + 1, std::memory_order_release);
    }

    loan_status claim_reliable(std::uint64_t &pos, claim_result &out) noexcept
    {
        for(;;)
        {
            // Gate BEFORE winning the position so reliable never burns a slot it
            // cannot fill: the slowest cursor must have drained the prior occupant.
            if(!consumers_passed(pos))
                return loan_status::congested;
            if(!m_header->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                continue;

            // The position is now ours and contiguous. A consumer may still hold a
            // pin on the prior occupant's aliased read; announce the claim and wait
            // the pin out (Disruptor gate -- reliable keeps FIFO contiguity, the pin
            // is transient). A won position is never abandoned: it resolves to ok
            // once the pin clears, or to a recognized skip-tombstone if the pin holder
            // is wedged (keeping the sequence contiguous so no consumer wedges on it).
            return clear_pin_or_skip(pos, out);
        }
    }

    // Waits out the transient take pin on a freshly-won reliable position. Returns ok
    // the moment no live pin remains (the unpinned first turn is byte-identical to the
    // old happy path). The no-progress budget RESETS whenever the slowest consumer
    // cursor advances, so a live-but-slow pin holder blocks losslessly however slow;
    // only a wedged holder (no cursor motion across the whole window) trips it, and
    // then the position is converted to a skip-tombstone (NOT abandoned) so contiguity
    // holds and the bail is OBSERVABLE as congested rather than a silent FIFO hole.
    loan_status clear_pin_or_skip(std::uint64_t pos, claim_result &out) noexcept
    {
        std::uint64_t last_seen = slowest_consumer_position();
        for(std::uint32_t stalled = 0; !announce_and_check_pin(pos); ++stalled)
        {
            const std::uint64_t now = slowest_consumer_position();
            if(now != last_seen)
            {
                last_seen = now;
                stalled   = 0; // the pin holder is alive: keep waiting losslessly
            }
            else if(stalled >= k_pin_clear_spin_budget)
            {
                stamp_skip(pos);
                return loan_status::congested;
            }
            cpu_relax();
        }
        out.position = pos;
        out.slab     = slot_span(pos);
        return loan_status::ok;
    }

    // NOLINTNEXTLINE(readability-function-size)
    loan_status claim_best_effort(std::uint64_t &pos, claim_result &out) noexcept
    {
        for(std::uint64_t skipped = 0;;)
        {
            // Only recycle a slot whose prior occupant is committed; an in-flight
            // peer owns it otherwise. A stale `pos` reloads on the next CAS failure.
            if(!occupant_committed(pos))
            {
                pos = m_header->enqueue_pos.load(std::memory_order_relaxed);
                continue;
            }
            if(!m_header->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed))
                continue;

            // Won the head; announce the claim and check the pin. Unpinned -> write.
            if(announce_and_check_pin(pos))
            {
                out.position = pos;
                out.slab     = slot_span(pos);
                return loan_status::ok;
            }
            // Pinned by a live take: best-effort donates this position (leaving a
            // recognizable tombstone) and tries the next. A full lap of pinned cells
            // is the bounded congested fallback (at most k_max_consumers live pins).
            stamp_skip(pos);
            if(++skipped >= m_cell_count)
                return loan_status::congested;
            pos = m_header->enqueue_pos.load(std::memory_order_relaxed);
        }
    }

    std::span<std::byte> m_control;
    std::span<std::byte> m_slab;
    control_header_t    *m_header{nullptr};
    cell_t              *m_cells{nullptr};
    std::uint64_t        m_cell_count{0};
    std::uint64_t        m_slot_capacity{0};
    std::uint64_t        m_mask{0};
    std::uint64_t        m_stride{0};
    std::uint64_t        m_consumer_capacity{0};
};

static_assert(broadcast_ring::k_cursor_idle == UINT64_MAX, "an idle (registered-but-not-gating) cursor must read as the max sentinel");

}

#endif

#ifndef HPP_GUARD_PLEXUS_SHM_BROADCAST_RING_H
#define HPP_GUARD_PLEXUS_SHM_BROADCAST_RING_H

#include "plexus/shm/ring_claim.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_attach.h"
#include "plexus/shm/ring_layout.h"
#include "plexus/shm/ring_cursors.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <span>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace plexus::shm {

// Lock-free, cross-process, offset-based MPMC broadcast ring: the Vyukov bounded MPMC queue
// adapted to DESCRIPTOR cells (each carries a slab byte offset + length + the per-cell sequence;
// the variable-size payload lives in a separate fixed-stride slab keyed 1:1 to the cells). The
// broadcast overlay (the Disruptor gating idea over Vyukov) gives every subscriber its own read
// cursor in the header's fixed-bound array; the producer reclamation gate is "the slowest
// registered consumer has passed this slot" (a k_cursor_idle sentinel does not gate). reliable
// blocks a cell until take_refcount==0 AND the slowest non-sentinel cursor is past it (a bounded
// spin, no kernel object); best-effort overwrites latest but SKIPS pinned cells.
//
// CRASH STANCE (crash-SAFE, NOT crash-LIVE): no cross-process lock is held, so a dying process
// never corrupts the ring; but a producer that dies after the enqueue CAS before the sequence
// release leaves a never-advancing cell, and a consumer that dies holding a take() leaves
// take_refcount>0.
class broadcast_ring
{
public:
    static constexpr std::uint64_t k_cursor_idle = shm::k_cursor_idle;

    // No-progress bound on the reliable pin-clear spin, in consecutive relax turns without slowest-cursor motion.
    static constexpr std::uint32_t k_pin_clear_spin_budget = 256;

    struct claim_result
    {
        std::uint64_t position{0};
        std::span<std::byte> slab;
    };

    struct consume_result
    {
        std::uint64_t position{0};
        std::span<const std::byte> slab;
    };

    broadcast_ring() noexcept
            : m_cells(nullptr)
            , m_mask(0)
            , m_stride(0)
            , m_header(nullptr)
            , m_cell_count(0)
            , m_slab()
            , m_slot_capacity(0)
            , m_control()
            , m_consumer_capacity(0)
    {
    }

    broadcast_ring(const broadcast_ring &)            = delete;
    broadcast_ring &operator=(const broadcast_ring &) = delete;
    broadcast_ring(broadcast_ring &&)                 = delete;
    broadcast_ring &operator=(broadcast_ring &&)      = delete;

    static loan_status create(std::span<std::byte> control, std::span<std::byte> slab, std::uint64_t cell_count, std::uint64_t slot_capacity, broadcast_ring &out,
                              std::uint64_t consumer_capacity = k_max_consumers) noexcept
    {
        return detail::ring_create(control, slab, cell_count, slot_capacity, out, consumer_capacity);
    }

    static loan_status attach(std::span<std::byte> control, std::span<std::byte> slab, broadcast_ring &out) noexcept
    {
        return detail::ring_attach(control, slab, out);
    }

    loan_status claim_with_policy(std::size_t size, io::reliability rel, io::congestion, claim_result &out) noexcept
    {
        return detail::claim_with_policy(*this, size, rel, out);
    }

    loan_status commit(std::uint64_t position, std::size_t filled_len) noexcept
    {
        return detail::ring_commit(*this, position, filled_len);
    }

    loan_status consume(std::uint64_t cursor, consume_result &out) noexcept
    {
        return detail::ring_consume(*this, cursor, out);
    }

    loan_status register_cursor(std::uint32_t &out_index) noexcept
    {
        return detail::register_cursor(*m_header, m_consumer_capacity, out_index);
    }

    void unregister_cursor(std::uint32_t index) noexcept
    {
        detail::unregister_cursor(*m_header, m_consumer_capacity, index);
    }

    void publish_cursor(std::uint32_t index, std::uint64_t position) noexcept
    {
        detail::publish_cursor(*m_header, m_consumer_capacity, index, position);
    }

    std::atomic<std::uint32_t> &refcount_at(std::uint64_t position) noexcept
    {
        return cell_at(position).take_refcount;
    }

    bool pin_if_current(std::uint64_t cursor) noexcept
    {
        return detail::pin_if_current(cell_at(cursor), cursor);
    }

    // A late-joining subscriber starts its cursor here so it sees messages published after it registers.
    std::uint64_t tail_position() const noexcept
    {
        return m_header->enqueue_pos.load(std::memory_order_acquire);
    }

    std::uint64_t slowest_consumer_position() const noexcept
    {
        return detail::slowest_consumer_position(*m_header);
    }

    control_header_t &header() noexcept
    {
        return *m_header;
    }

    std::uint64_t cell_count() const noexcept
    {
        return m_cell_count;
    }

    std::uint64_t slot_capacity() const noexcept
    {
        return m_slot_capacity;
    }

    // The largest index+1 ever registered, which bounds the reliable-reclaim scan length.
    std::uint32_t registered_high_water() const noexcept
    {
        return m_header->high_water.load(std::memory_order_acquire);
    }

private:
    cell_t *m_cells;
    std::uint64_t m_mask;
    std::uint64_t m_stride;
    control_header_t *m_header;
    std::uint64_t m_cell_count;
    std::span<std::byte> m_slab;
    std::uint64_t m_slot_capacity;
    std::span<std::byte> m_control;
    std::uint64_t m_consumer_capacity;

    template<typename R>
    friend void detail::bind_ring(R &, std::span<std::byte>, std::span<std::byte>, control_header_t *, cell_t *, std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t) noexcept;
    template<typename R>
    friend loan_status detail::claim_reliable(R &, std::uint64_t &, typename R::claim_result &) noexcept;
    template<typename R>
    friend loan_status detail::clear_pin_or_skip(R &, std::uint64_t, typename R::claim_result &) noexcept;
    template<typename R>
    friend loan_status detail::claim_best_effort(R &, std::uint64_t &, typename R::claim_result &) noexcept;
    template<typename R>
    friend loan_status detail::ring_commit(R &, std::uint64_t, std::size_t) noexcept;
    template<typename R>
    friend loan_status detail::ring_consume(R &, std::uint64_t, typename R::consume_result &) noexcept;

    cell_t &cell_at(std::uint64_t position) noexcept
    {
        return m_cells[position & m_mask];
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
};

static_assert(broadcast_ring::k_cursor_idle == UINT64_MAX, "an idle (registered-but-not-gating) cursor must read as the max sentinel");

}

#endif

#ifndef HPP_GUARD_PLEXUS_SHM_RING_ATTACH_H
#define HPP_GUARD_PLEXUS_SHM_RING_ATTACH_H

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"

#include <new>
#include <span>
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace plexus::shm::detail {

inline bool is_power_of_two(std::uint64_t v) noexcept
{
    return v != 0 && (v & (v - 1)) == 0;
}

// Every hot counter, config field and cursor slot is initialized; the magic stays unstamped.
inline control_header_t *init_header(std::span<std::byte> control, std::uint64_t cell_count, std::uint64_t slot_capacity, std::uint64_t consumer_capacity) noexcept
{
    auto *header = ::new(control.data()) control_header_t{};
    header->enqueue_pos.store(0, std::memory_order_relaxed);
    header->cell_count        = cell_count;
    header->slot_capacity     = slot_capacity;
    header->mask              = cell_count - 1;
    header->consumer_capacity = consumer_capacity;
    header->notify_generation.store(0, std::memory_order_relaxed);
    header->high_water.store(0, std::memory_order_relaxed);
    for(std::size_t i = 0; i < k_max_consumers; ++i)
    {
        header->cursors[i].position.store(k_cursor_idle, std::memory_order_relaxed);
        header->cursors[i].active.store(0, std::memory_order_relaxed);
    }
    return header;
}

// Initializes every cell's sequence to its index per the Vyukov init.
inline cell_t *init_cells(std::span<std::byte> control, std::uint64_t cell_count) noexcept
{
    auto *cells = ::new(control.data() + sizeof(control_header_t)) cell_t[cell_count];
    for(std::uint64_t i = 0; i < cell_count; ++i)
    {
        cells[i].sequence.store(i, std::memory_order_relaxed);
        cells[i].payload_offset.store(0, std::memory_order_relaxed);
        cells[i].payload_len.store(0, std::memory_order_relaxed);
        cells[i].take_refcount.store(0, std::memory_order_relaxed);
    }
    return cells;
}

template<typename Ring>
void bind_ring(Ring &out, std::span<std::byte> control, std::span<std::byte> slab, control_header_t *header, cell_t *cells, std::uint64_t cell_count, std::uint64_t slot_capacity,
               std::uint64_t mask, std::uint64_t consumer_capacity) noexcept
{
    out.m_control           = control;
    out.m_slab              = slab;
    out.m_header            = header;
    out.m_cells             = cells;
    out.m_cell_count        = cell_count;
    out.m_slot_capacity     = slot_capacity;
    out.m_mask              = mask;
    out.m_stride            = round_up_8(slot_capacity);
    out.m_consumer_capacity = consumer_capacity;
}

// cell_count must be a power of two; the spans must already be sized for the geometry.
template<typename Ring>
loan_status ring_create(std::span<std::byte> control, std::span<std::byte> slab, std::uint64_t cell_count, std::uint64_t slot_capacity, Ring &out,
                        std::uint64_t consumer_capacity) noexcept
{
    if(!is_power_of_two(cell_count) || slot_capacity == 0)
        return loan_status::rejected;
    if(consumer_capacity == 0 || consumer_capacity > k_max_consumers)
        return loan_status::rejected;
    if(control.size() < control_region_bytes(cell_count) || slab.size() < slab_region_bytes(cell_count, slot_capacity))
        return loan_status::rejected;

    control_header_t *header = init_header(control, cell_count, slot_capacity, consumer_capacity);
    cell_t *cells            = init_cells(control, cell_count);
    // Stamped last with release, so a joiner whose acquire load sees the stamp sees every field and cell above.
    header->magic.store(k_ring_magic, std::memory_order_release);

    bind_ring(out, control, slab, header, cells, cell_count, slot_capacity, cell_count - 1, consumer_capacity);
    return loan_status::ok;
}

// Re-reads + bounds-checks the config from the control header (never trusts a peer's separate
// value): magic first, then power-of-two cell_count, mask == cell_count - 1, and both region
// sizes. A zero magic is a ring its creator has not stamped yet; a foreign or layout-incompatible region is rejected.
template<typename Ring>
loan_status ring_attach(std::span<std::byte> control, std::span<std::byte> slab, Ring &out) noexcept
{
    if(control.size() < sizeof(control_header_t))
        return loan_status::rejected;

    auto *header              = std::launder(reinterpret_cast<control_header_t *>(control.data()));
    const std::uint64_t magic = header->magic.load(std::memory_order_acquire);
    if(magic == 0)
        return loan_status::unstamped;
    if(magic != k_ring_magic || !is_power_of_two(header->cell_count) || header->slot_capacity == 0)
        return loan_status::rejected;
    if(header->mask != header->cell_count - 1)
        return loan_status::rejected;
    if(header->consumer_capacity == 0 || header->consumer_capacity > k_max_consumers)
        return loan_status::rejected;

    const std::uint64_t stride = round_up_8(header->slot_capacity);
    if(control.size() < control_region_bytes(header->cell_count) || slab.size() < header->cell_count * stride)
        return loan_status::rejected;

    auto *cells = std::launder(reinterpret_cast<cell_t *>(control.data() + sizeof(control_header_t)));
    bind_ring(out, control, slab, header, cells, header->cell_count, header->slot_capacity, header->mask, header->consumer_capacity);
    return loan_status::ok;
}

}

#endif

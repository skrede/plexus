#ifndef HPP_GUARD_PLEXUS_SHM_RING_ATTACH_H
#define HPP_GUARD_PLEXUS_SHM_RING_ATTACH_H

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_layout.h"

#include "plexus/node_id.h"

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
inline control_header_t *init_header(std::span<std::byte> control, std::span<std::byte> slab, std::uint64_t cell_count, std::uint64_t slot_capacity, std::uint64_t consumer_capacity,
                                     const node_id &publisher) noexcept
{
    auto *header = ::new(control.data()) control_header_t{};
    header->enqueue_pos.store(0, std::memory_order_relaxed);
    header->cell_count        = cell_count;
    header->slot_capacity     = slot_capacity;
    header->mask              = cell_count - 1;
    header->consumer_capacity = consumer_capacity;
    header->control_bytes     = control.size();
    header->slab_bytes        = slab.size();
    header->publisher         = publisher;
    header->notify_generation.store(0, std::memory_order_relaxed);
    header->high_water.store(0, std::memory_order_relaxed);
    for(std::size_t i = 0; i < k_max_consumers; ++i)
    {
        header->cursors[i].position.store(k_cursor_idle, std::memory_order_relaxed);
        header->cursors[i].active.store(0, std::memory_order_relaxed);
    }
    return header;
}

// Constructs each cell in place and initializes its sequence to its index per the Vyukov init.
inline cell_t *init_cells(std::span<std::byte> control, std::uint64_t cell_count) noexcept
{
    std::byte *first = control.data() + sizeof(control_header_t);
    for(std::uint64_t i = 0; i < cell_count; ++i)
    {
        auto *cell = ::new(first + i * sizeof(cell_t)) cell_t{};
        cell->sequence.store(i, std::memory_order_relaxed);
        cell->payload_len.store(0, std::memory_order_relaxed);
        cell->take_refcount.store(0, std::memory_order_relaxed);
    }
    return std::launder(reinterpret_cast<cell_t *>(first));
}

inline bool geometry_bounded(std::uint64_t cell_count, std::uint64_t slot_capacity) noexcept
{
    return is_power_of_two(cell_count) && cell_count <= k_max_cells && slot_capacity != 0 && slot_capacity <= k_max_slot_capacity;
}

// A peer-written header is checked before any size is computed from it: the geometry maxima first, so no product
// below can wrap, then each mapped region must be exactly the size its creator recorded and cover the geometry.
inline loan_status header_admits(const control_header_t &h, std::size_t control_size, std::size_t slab_size, const node_id &publisher) noexcept
{
    if(!geometry_bounded(h.cell_count, h.slot_capacity) || h.mask != h.cell_count - 1)
        return loan_status::rejected;
    if(h.consumer_capacity == 0 || h.consumer_capacity > k_max_consumers)
        return loan_status::rejected;
    if(h.control_bytes != control_size || h.slab_bytes != slab_size)
        return loan_status::rejected;
    if(h.control_bytes < control_region_bytes(h.cell_count) || h.slab_bytes < h.cell_count * round_up_8(h.slot_capacity))
        return loan_status::rejected;
    return h.publisher == publisher ? loan_status::ok : loan_status::rejected;
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

// cell_count must be a power of two; the spans must already be sized for the geometry and are recorded as mapped.
template<typename Ring>
loan_status ring_create(std::span<std::byte> control, std::span<std::byte> slab, std::uint64_t cell_count, std::uint64_t slot_capacity, const node_id &publisher, Ring &out,
                        std::uint64_t consumer_capacity) noexcept
{
    if(!geometry_bounded(cell_count, slot_capacity))
        return loan_status::rejected;
    if(consumer_capacity == 0 || consumer_capacity > k_max_consumers)
        return loan_status::rejected;
    if(control.size() < control_region_bytes(cell_count) || slab.size() < slab_region_bytes(cell_count, slot_capacity))
        return loan_status::rejected;

    control_header_t *header = init_header(control, slab, cell_count, slot_capacity, consumer_capacity, publisher);
    cell_t *cells            = init_cells(control, cell_count);
    // Stamped last with release, so a joiner whose acquire load sees the stamp sees every field and cell above.
    header->magic.store(k_ring_magic, std::memory_order_release);

    bind_ring(out, control, slab, header, cells, cell_count, slot_capacity, cell_count - 1, consumer_capacity);
    return loan_status::ok;
}

// Re-reads + bounds-checks the config from the control header (never trusts a peer's separate value): magic first, then
// the header checks. A zero magic is a ring its creator has not stamped yet; a foreign, layout-incompatible or
// mis-sized region, or one another publisher owns, is rejected.
template<typename Ring>
loan_status ring_attach(std::span<std::byte> control, std::span<std::byte> slab, const node_id &publisher, Ring &out) noexcept
{
    if(control.size() < sizeof(control_header_t))
        return loan_status::rejected;

    auto *header              = std::launder(reinterpret_cast<control_header_t *>(control.data()));
    const std::uint64_t magic = header->magic.load(std::memory_order_acquire);
    if(magic == 0)
        return loan_status::unstamped;
    if(magic == k_ring_retired)
        return loan_status::retired;
    if(magic != k_ring_magic)
        return loan_status::rejected;
    if(const loan_status admitted = header_admits(*header, control.size(), slab.size(), publisher); admitted != loan_status::ok)
        return admitted;

    auto *cells = std::launder(reinterpret_cast<cell_t *>(control.data() + sizeof(control_header_t)));
    bind_ring(out, control, slab, header, cells, header->cell_count, header->slot_capacity, header->mask, header->consumer_capacity);
    return loan_status::ok;
}

}

#endif

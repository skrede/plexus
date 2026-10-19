#ifndef HPP_GUARD_PLEXUS_SHM_LOANED_BUFFER_H
#define HPP_GUARD_PLEXUS_SHM_LOANED_BUFFER_H

#include "plexus/shm/broadcast_ring.h"

#include <span>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <functional>

namespace plexus::shm {

class slot_publisher;

// Move-only producer handle over the slot one claim won: a writable byte window and the ring position its publish
// commits. Dropping it unpublished abandons the position, so the ring stamps a skip that every consumer steps past.
class loaned_buffer
{
public:
    loaned_buffer() noexcept
            : m_filled(0)
            , m_position(0)
            , m_slot()
            , m_ring()
    {
    }

    ~loaned_buffer()
    {
        reclaim();
    }

    loaned_buffer(const loaned_buffer &)            = delete;
    loaned_buffer &operator=(const loaned_buffer &) = delete;

    loaned_buffer(loaned_buffer &&other) noexcept
            : m_filled(other.m_filled)
            , m_position(other.m_position)
            , m_slot(other.m_slot)
            , m_ring(other.m_ring)
    {
        other.disengage();
    }

    loaned_buffer &operator=(loaned_buffer &&other) noexcept
    {
        if(this == &other)
            return *this;

        reclaim();
        m_filled   = other.m_filled;
        m_position = other.m_position;
        m_slot     = other.m_slot;
        m_ring     = other.m_ring;
        other.disengage();
        return *this;
    }

    // The writable, 8-aligned slot window.
    std::span<std::byte> bytes() const
    {
        assert(m_ring && "loaned_buffer::bytes() on an empty or moved-from handle");
        return m_slot;
    }

    std::size_t capacity() const noexcept
    {
        return m_slot.size();
    }

    void set_filled(std::size_t filled) noexcept
    {
        m_filled = filled;
    }

    std::size_t filled() const noexcept
    {
        return m_filled;
    }

private:
    friend class slot_publisher;

    loaned_buffer(broadcast_ring &ring, std::span<std::byte> slot, std::uint64_t position) noexcept
            : m_filled(0)
            , m_position(position)
            , m_slot(slot)
            , m_ring(ring)
    {
    }

    void reclaim() noexcept
    {
        if(m_ring)
            m_ring->get().abandon(m_position);
        disengage();
    }

    void disengage() noexcept
    {
        m_filled = 0;
        m_slot   = {};
        m_ring.reset();
    }

    std::size_t m_filled;
    std::uint64_t m_position;
    std::span<std::byte> m_slot;
    std::optional<std::reference_wrapper<broadcast_ring>> m_ring;
};

}

#endif

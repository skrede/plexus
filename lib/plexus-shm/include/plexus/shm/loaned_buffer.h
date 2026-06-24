#ifndef HPP_GUARD_PLEXUS_SHM_LOANED_BUFFER_H
#define HPP_GUARD_PLEXUS_SHM_LOANED_BUFFER_H

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>

namespace plexus::shm {

class slot_publisher;

namespace test {
struct handle_test_access;
}

// Move-only producer handle: a writable byte window into the slot a loan claimed, plus
// an opaque slot ticket (the ring position whose lap the publish commits). reclaim()
// abandons an unpublished claim; it is idempotent and nulls every field so a second call
// (move-assign or post-publish destructor) does nothing -- it must never double-publish.
class loaned_buffer
{
public:
    loaned_buffer() = default;

    ~loaned_buffer()
    {
        reclaim();
    }

    loaned_buffer(const loaned_buffer &)            = delete;
    loaned_buffer &operator=(const loaned_buffer &) = delete;

    loaned_buffer(loaned_buffer &&other) noexcept
            : m_slot(other.m_slot)
            , m_capacity(other.m_capacity)
            , m_filled(other.m_filled)
            , m_position(other.m_position)
    {
        other.m_slot     = nullptr;
        other.m_capacity = 0;
        other.m_filled   = 0;
    }

    loaned_buffer &operator=(loaned_buffer &&other) noexcept
    {
        if(this == &other)
            return *this;

        reclaim();

        m_slot     = other.m_slot;
        m_capacity = other.m_capacity;
        m_filled   = other.m_filled;
        m_position = other.m_position;

        other.m_slot     = nullptr;
        other.m_capacity = 0;
        other.m_filled   = 0;
        return *this;
    }

    // The writable, 8-aligned slot window.
    std::span<std::byte> bytes() const
    {
        assert(m_slot != nullptr && "loaned_buffer::bytes() on an empty or moved-from handle");
        return {m_slot, m_capacity};
    }

    std::size_t capacity() const noexcept
    {
        return m_capacity;
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
    friend struct ::plexus::shm::test::handle_test_access;

    loaned_buffer(std::byte *slot, std::size_t capacity, std::uint64_t position) noexcept
            : m_slot(slot)
            , m_capacity(capacity)
            , m_position(position)
    {
    }

    // The cell sequence is only advanced by publish(), so a never-published slot is
    // simply left for the producer's next loan.
    void reclaim() noexcept
    {
        m_slot     = nullptr;
        m_capacity = 0;
        m_filled   = 0;
    }

    std::byte *m_slot{nullptr};
    std::size_t m_capacity{0};
    std::size_t m_filled{0};
    std::uint64_t m_position{0};
};

}

#endif

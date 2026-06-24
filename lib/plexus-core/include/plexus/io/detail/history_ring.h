#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_HISTORY_RING_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_HISTORY_RING_H

#include <span>
#include <vector>
#include <cstddef>
#include <algorithm>

namespace plexus::io::detail {

// A single-owner, in-process bounded ring of N owned complete-frame buffers. Slots are grown
// once at resize_to and reused, so push() is allocation-free on the steady publish path.
class history_ring
{
public:
    history_ring()
            : m_head(0)
            , m_count(0)
    {
    }

    void resize_to(std::size_t capacity)
    {
        const std::size_t n = capacity == 0 ? 1 : capacity;
        if(m_slots.size() != n)
            m_slots.resize(n);
    }

    void push(std::span<const std::byte> frame)
    {
        m_slots[m_head].assign(frame.begin(), frame.end());
        m_head  = (m_head + 1) % m_slots.size();
        m_count = std::min(m_count + 1, m_slots.size());
    }

    bool empty() const
    {
        return m_count == 0;
    }

    std::size_t count() const
    {
        return m_count;
    }

    std::size_t capacity() const
    {
        return m_slots.size();
    }

    const std::vector<std::byte> &newest() const
    {
        return m_slots[(m_head + m_slots.size() - 1) % m_slots.size()];
    }

    const std::vector<std::byte> &oldest_to_newest(std::size_t i) const
    {
        return m_slots[(m_head + m_slots.size() - m_count + i) % m_slots.size()];
    }

private:
    std::vector<std::vector<std::byte>> m_slots;
    std::size_t m_head;
    std::size_t m_count;
};

}

#endif

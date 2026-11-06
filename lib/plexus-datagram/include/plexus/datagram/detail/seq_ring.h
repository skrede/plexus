#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_SEQ_RING_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_SEQ_RING_H

#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::datagram::detail {

// A base-relative ring index: a sequence's slot is its distance from the base sequence, offset from the base slot, so
// held sequences never alias for a window that does not divide the 16-bit sequence space.
template<typename T>
class seq_ring
{
public:
    using iterator = typename std::vector<T>::iterator;

    seq_ring(std::size_t window, std::uint16_t base_seq)
            : m_slots(window)
            , m_base_index(0)
            , m_base_seq(base_seq)
    {
    }

    T &at(std::uint16_t seq) noexcept
    {
        return m_slots[(m_base_index + distance(seq)) % m_slots.size()];
    }

    bool holds(std::uint16_t seq) const noexcept
    {
        return distance(seq) < m_slots.size();
    }

    void advance() noexcept
    {
        ++m_base_seq;
        m_base_index = (m_base_index + 1) % m_slots.size();
    }

    std::uint16_t base() const noexcept
    {
        return m_base_seq;
    }

    std::size_t window() const noexcept
    {
        return m_slots.size();
    }

    iterator begin() noexcept
    {
        return m_slots.begin();
    }

    iterator end() noexcept
    {
        return m_slots.end();
    }

private:
    std::vector<T> m_slots;
    std::size_t m_base_index;
    std::uint16_t m_base_seq;

    std::size_t distance(std::uint16_t seq) const noexcept
    {
        return static_cast<std::uint16_t>(seq - m_base_seq);
    }
};

}

#endif

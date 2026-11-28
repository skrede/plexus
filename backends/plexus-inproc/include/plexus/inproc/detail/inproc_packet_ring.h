#ifndef HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_PACKET_RING_H
#define HPP_GUARD_PLEXUS_INPROC_DETAIL_INPROC_PACKET_RING_H

#include "plexus/io/object_carrier.h"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

// In plexus::detail beside dispatch_packet, for the same reason: a plexus::inproc::detail would shadow the bus's bare
// detail::move_only_function lookups.
namespace plexus::detail {

enum class inproc_packet_kind : std::uint8_t
{
    data,
    close,
    object
};

// Packets address their target by the registration key, never the endpoint value: per-enqueue
// string assigns + a per-channel string compare were measurable on the publish loop.
struct inproc_packet
{
    std::uint64_t to_key{0};
    inproc_packet_kind kind{inproc_packet_kind::data};
    std::uint64_t from_key{0};
    std::vector<std::byte> data;
    io::object_carrier carrier{};
};

// The FIFO rides a grown-once ring of reusable packet slots so the steady publish loop is
// alloc-free: the ring doubles only on overflow (never shrinks) and each slot reuses its data
// vector's capacity via assign. The head packet leaves its slot by move and its buffer returns as
// the ring's one spare, so a send from the delivery callback never touches the packet being
// delivered and the steady state stays alloc-free.
class inproc_packet_ring
{
public:
    inproc_packet_ring()
            : m_head(0)
            , m_size(0)
            , m_spare()
            , m_ring()
    {
    }

    bool empty() const noexcept
    {
        return m_size == 0;
    }

    inproc_packet &push_slot()
    {
        if(m_size == m_ring.size())
            grow();
        const std::size_t tail = (m_head + m_size) % m_ring.size();
        ++m_size;
        return m_ring[tail];
    }

    inproc_packet take_front()
    {
        inproc_packet &slot = m_ring[m_head];
        inproc_packet pkt   = std::move(slot);
        slot.data           = std::exchange(m_spare, {});
        m_head              = (m_head + 1) % m_ring.size();
        --m_size;
        return pkt;
    }

    void recycle(std::vector<std::byte> &&buffer) noexcept
    {
        m_spare = std::move(buffer);
    }

private:
    std::size_t m_head;
    std::size_t m_size;
    std::vector<std::byte> m_spare;
    std::vector<inproc_packet> m_ring;

    void grow()
    {
        const std::size_t old_cap = m_ring.size();
        const std::size_t new_cap = old_cap == 0 ? 8 : old_cap * 2;
        std::vector<inproc_packet> next(new_cap);
        for(std::size_t i = 0; i < m_size; ++i)
            next[i] = std::move(m_ring[(m_head + i) % old_cap]);
        m_ring = std::move(next);
        m_head = 0;
    }
};

}

#endif

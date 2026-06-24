#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_HISTORY_RING_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_HISTORY_RING_H

#include <span>
#include <vector>
#include <cstddef>
#include <algorithm>

namespace plexus::io::detail {

// A single-owner, in-process bounded ring of N owned complete-frame buffers — the
// retain seam for a KEEP_LAST-N latched topic. It borrows only the head/count/
// overwrite-oldest VOCABULARY of the shared-memory broadcast_ring; it is NOT a
// cross-process structure: plain indices, no atomics, no shared_from_this, no MPMC
// machinery. The slots are grown ONCE at declare from the topic's depth and reused
// thereafter, so push() is allocation-free on the steady publish path (assign()
// into a slot whose capacity a prior push already grew).
//
// A capacity-1 ring is byte-identical in behavior to the pre-ring single retained
// slot: push overwrites the one slot and replay yields the single newest frame
// (last-writer-wins per topic_hash).
class history_ring
{
public:
    // Size the ring to N slots ONCE (cold, called from retain before the first
    // push when the declared depth is known). A re-size to the same N is a no-op,
    // so the slots' grown capacity survives; capacity 0 is treated as 1.
    void resize_to(std::size_t capacity)
    {
        const std::size_t n = capacity == 0 ? 1 : capacity;
        if(m_slots.size() != n)
            m_slots.resize(n);
    }

    // Push a complete framed buffer into the next slot (round-robin), reusing the
    // slot's grown capacity — alloc-free once each slot has been touched once.
    // head advances mod N; count saturates at N (the oldest frame is overwritten).
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

    // The most-recently-pushed frame (durability=latest). Precondition: count > 0.
    const std::vector<std::byte> &newest() const
    {
        return m_slots[(m_head + m_slots.size() - 1) % m_slots.size()];
    }

    // The i-th frame in oldest->newest order for i in [0, count). The forwarder
    // loops i and sends each, so a durability=all / fetch_latched replay walks the
    // retained window oldest-first.
    const std::vector<std::byte> &oldest_to_newest(std::size_t i) const
    {
        return m_slots[(m_head + m_slots.size() - m_count + i) % m_slots.size()];
    }

private:
    std::vector<std::vector<std::byte>> m_slots;
    std::size_t                         m_head  = 0;
    std::size_t                         m_count = 0;
};

}

#endif

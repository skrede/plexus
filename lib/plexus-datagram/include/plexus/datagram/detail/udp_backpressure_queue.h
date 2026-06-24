#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_BACKPRESSURE_QUEUE_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_UDP_BACKPRESSURE_QUEUE_H

#include <span>
#include <deque>
#include <vector>
#include <cstddef>

namespace plexus::datagram::detail {

// A bounded FIFO of owned frames a reliable channel parks when its ARQ send window is full, drained
// by the next ack that frees a window slot; an admit past the cap is refused so the queue never
// grows unbounded. The cap accounts the summed payload bytes, not the entry count (a single large
// fragment must trip the budget), and admission is compare-before-add so a crafted sequence cannot
// overflow the running total past the cap. Each parked frame carries its FRAGMENTED disposition so
// the drain re-submits it as a fragment (else the peer routes the payload to whole-message delivery
// instead of the reassembler).
class udp_backpressure_queue
{
public:
    explicit udp_backpressure_queue(std::size_t byte_cap) noexcept
            : m_byte_cap(byte_cap == 0 ? std::size_t{1} : byte_cap)
    {
    }

    // Returns false if admitting the frame would carry the parked byte total past the cap (the
    // caller surfaces the stall signal); the bytes are copied into an owned slot otherwise.
    bool admit(std::span<const std::byte> frame, bool fragmented)
    {
        if(!admits(frame.size()))
            return false;
        m_bytes += frame.size();
        m_queue.push_back(entry{{frame.begin(), frame.end()}, fragmented});
        return true;
    }

    bool empty() const noexcept
    {
        return m_queue.empty();
    }
    std::size_t size() const noexcept
    {
        return m_queue.size();
    }
    std::size_t queued_bytes() const noexcept
    {
        return m_bytes;
    }

    std::size_t capacity() const noexcept
    {
        return m_byte_cap;
    }

    // A non-owning view valid until the next pop/admit.
    std::span<const std::byte> front() const
    {
        return std::span<const std::byte>{m_queue.front().bytes};
    }
    bool front_fragmented() const noexcept
    {
        return m_queue.front().fragmented;
    }
    void pop_front()
    {
        m_bytes -= m_queue.front().bytes.size();
        m_queue.pop_front();
    }

private:
    struct entry
    {
        std::vector<std::byte> bytes;
        bool fragmented;
    };

    // Compare-before-add: a frame fits only when the cap is not already met AND its size is within
    // the remaining budget (cap - bytes, with bytes < cap by the first clause, cannot wrap).
    bool admits(std::size_t size) const noexcept
    {
        return m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes;
    }

    std::size_t m_byte_cap;
    std::size_t m_bytes{0};
    std::deque<entry> m_queue;
};

}

#endif

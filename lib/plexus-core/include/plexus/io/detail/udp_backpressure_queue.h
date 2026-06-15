#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UDP_BACKPRESSURE_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UDP_BACKPRESSURE_QUEUE_H

#include <span>
#include <deque>
#include <vector>
#include <cstddef>

namespace plexus::io::detail {

// The congestion=block backpressure queue: a BOUNDED FIFO of owned frames that a reliable
// channel parks when its ARQ send window is full, drained by the next ack that frees a
// window slot. A sans-IO unit — it owns no executor, no timer, no socket — so it is
// independently testable and keeps udp_channel focused on the ARQ wiring. The cap is fixed
// at setup; an admit past the cap is REFUSED (the channel surfaces the stall signal) so the
// queue never grows unbounded. There is NO steady-state hot-path allocation beyond the
// deque's own node churn, which reuses freed nodes as frames drain.
//
// The cap accounts the SUMMED PAYLOAD BYTES of the parked frames, not the entry count: a
// single large fragment must trip the byte budget even though it is one entry, and the
// E-001 paced reliable splitter relies on a byte bound to keep the in-flight set bounded
// regardless of how a large message divides into fragments. Admission is compare-BEFORE-add
// (cap - bytes, no wrap) so a crafted sequence of large frames cannot overflow the running
// total past the cap and re-admit unboundedly.
//
// Each parked frame carries its FRAGMENTED disposition alongside the bytes: a fragment of a
// large message parks here when the send window is full, and the flag must survive the round
// trip through the queue so the drain re-submits it as a fragment (without it the peer would
// route the in-order payload to whole-message delivery instead of the reassembler).
class udp_backpressure_queue
{
public:
    explicit udp_backpressure_queue(std::size_t byte_cap) noexcept
        : m_byte_cap(byte_cap == 0 ? std::size_t{1} : byte_cap)
    {
    }

    // Park a window-full frame and its FRAGMENTED disposition. Returns false if admitting it
    // would carry the parked byte total past the cap (the caller surfaces the stall signal);
    // the bytes are copied into an owned slot otherwise.
    bool admit(std::span<const std::byte> frame, bool fragmented)
    {
        if(!admits(frame.size()))
            return false;
        m_bytes += frame.size();
        m_queue.push_back(entry{{frame.begin(), frame.end()}, fragmented});
        return true;
    }

    [[nodiscard]] bool empty() const noexcept { return m_queue.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return m_queue.size(); }
    [[nodiscard]] std::size_t queued_bytes() const noexcept { return m_bytes; }

    // The configured byte cap: the bound the egress scheduler's low-water gate tracks so the
    // band hand-off and this queue's admission stay in lockstep.
    [[nodiscard]] std::size_t capacity() const noexcept { return m_byte_cap; }

    // The front parked frame (a non-owning view valid until the next pop/admit).
    [[nodiscard]] std::span<const std::byte> front() const { return std::span<const std::byte>{m_queue.front().bytes}; }
    [[nodiscard]] bool front_fragmented() const noexcept { return m_queue.front().fragmented; }
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

    // Compare-before-add: a frame fits only when the cap is not already met AND its size
    // is within the remaining budget (cap - bytes, both unsigned with bytes < cap by the
    // first clause), so no crafted size can wrap the comparison.
    [[nodiscard]] bool admits(std::size_t size) const noexcept
    {
        return m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes;
    }

    std::size_t m_byte_cap;
    std::size_t m_bytes{0};
    std::deque<entry> m_queue;
};

}

#endif

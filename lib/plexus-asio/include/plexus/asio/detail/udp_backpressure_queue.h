#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_BACKPRESSURE_QUEUE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_BACKPRESSURE_QUEUE_H

#include <span>
#include <deque>
#include <vector>
#include <cstddef>

namespace plexus::asio::detail {

// The congestion=block backpressure queue: a BOUNDED FIFO of owned frames that a reliable
// channel parks when its ARQ send window is full, drained by the next ack (T-15-13). A
// sans-IO unit — it owns no executor, no timer, no socket — so it is independently
// testable and keeps udp_channel focused on the ARQ wiring. The cap is fixed at setup; an
// admit past the cap is REFUSED (the channel surfaces the stall signal) so the queue never
// grows unbounded. There is NO steady-state hot-path allocation beyond the deque's own
// node churn, which reuses freed nodes as frames drain.
class udp_backpressure_queue
{
public:
    explicit udp_backpressure_queue(std::size_t cap) noexcept
        : m_cap(cap == 0 ? std::size_t{1} : cap)
    {
    }

    // Park a window-full frame. Returns false if the queue is at its cap (the caller
    // surfaces the stall signal); the bytes are copied into an owned slot otherwise.
    bool admit(std::span<const std::byte> frame)
    {
        if(m_queue.size() >= m_cap)
            return false;
        m_queue.emplace_back(frame.begin(), frame.end());
        return true;
    }

    [[nodiscard]] bool empty() const noexcept { return m_queue.empty(); }
    [[nodiscard]] std::size_t size() const noexcept { return m_queue.size(); }

    // The front parked frame (a non-owning view valid until the next pop/admit).
    [[nodiscard]] std::span<const std::byte> front() const { return std::span<const std::byte>{m_queue.front()}; }
    void pop_front() { m_queue.pop_front(); }

private:
    std::size_t m_cap;
    std::deque<std::vector<std::byte>> m_queue;
};

}

#endif

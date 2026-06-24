#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_SEND_QUEUE_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_SEND_QUEUE_H

#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <limits>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::datagram::detail {

// A sans-IO serial outbound discipline. enqueue() copies the caller's bytes into a block-owned
// node: the backend send-sink is a non-owning view and the async op does not copy, so a caller
// scratch reused on the next send would corrupt an in-flight datagram. The queue drains serially
// (at most one send-sink invocation outstanding). A finite cap accounts the summed payload bytes,
// not the entry count, and admission is compare-before-add so a crafted sequence of large frames
// cannot wrap the running total past the cap.
template<typename Endpoint>
class send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    using completion = plexus::detail::move_only_function<void(bool)>;
    using send_sink  = plexus::detail::move_only_function<void(std::span<const std::byte>, const Endpoint &, completion)>;

    explicit send_queue(send_sink sink, std::size_t byte_cap = unbounded)
            : m_sink(std::move(sink))
            , m_byte_cap(byte_cap)
    {
    }

    // Returns false (admitting nothing) when this frame would carry the queued byte total past the
    // cap; the check is compare-before-add so the running total can never overflow past the cap.
    bool enqueue(std::span<const std::byte> bytes, const Endpoint &dest)
    {
        if(!admits(bytes.size()))
            return false;
        m_bytes += bytes.size();
        m_queue.push_back(node{take_buffer(bytes), dest});
        if(!m_sending)
            drive();
        return true;
    }

    bool full() const noexcept
    {
        return m_bytes >= m_byte_cap;
    }

    std::size_t size() const noexcept
    {
        return m_queue.size();
    }

    std::size_t queued_bytes() const noexcept
    {
        return m_bytes;
    }

    bool sending() const noexcept
    {
        return m_sending;
    }

    // A completion firing after close is a guarded no-op; close() is terminal (m_open never
    // re-arms), so the recycled-buffer pool is released here too.
    void close()
    {
        m_open    = false;
        m_sending = false;
        m_queue.clear();
        m_free_buffers.clear();
        m_bytes = 0;
    }

private:
    struct node
    {
        std::vector<std::byte> bytes;
        Endpoint dest;
    };

    // Compare-before-add: a frame fits only when the cap is not already met AND its size is within
    // the remaining budget (cap - bytes, with bytes < cap by the first clause, cannot overflow).
    bool admits(std::size_t size) const noexcept
    {
        return m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes;
    }

    // Pull a recycled buffer (reusing its heap capacity) or grow one when the freelist is empty, so
    // a burst that re-fills the queue allocates nothing once the freelist has warmed.
    std::vector<std::byte> take_buffer(std::span<const std::byte> bytes)
    {
        std::vector<std::byte> buf;
        if(!m_free_buffers.empty())
        {
            buf = std::move(m_free_buffers.back());
            m_free_buffers.pop_back();
        }
        buf.assign(bytes.begin(), bytes.end());
        return buf;
    }

    // Return a drained buffer to the freelist, dropping it when the freelist is already at its
    // node-count bound so the spare pool (which the byte cap does not bound) cannot grow unbounded.
    void recycle_buffer(std::vector<std::byte> &&buf)
    {
        if(m_free_buffers.size() < k_max_free_buffers)
            m_free_buffers.push_back(std::move(buf));
    }

    // Send the front node; on completion pop it, recycle its buffer, and chain the next. At most
    // one send-sink invocation is outstanding, so a node's bytes stay valid until its completion.
    void drive()
    {
        if(m_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending         = true;
        const auto &front = m_queue.front();
        m_sink(std::span<const std::byte>{front.bytes}, front.dest,
               [this](bool /*ok*/)
               {
                   if(!m_open)
                       return;
                   m_bytes -= m_queue.front().bytes.size();
                   recycle_buffer(std::move(m_queue.front().bytes));
                   m_queue.pop_front();
                   drive();
               });
    }

    // The spare-buffer pool's node-count ceiling: the byte cap bounds the live queue, this
    // independently bounds the idle pool so a momentary deep burst cannot leave an unbounded pool.
    static constexpr std::size_t k_max_free_buffers = 64;

    send_sink m_sink;
    std::size_t m_byte_cap;
    std::deque<node> m_queue;
    std::vector<std::vector<std::byte>> m_free_buffers;
    std::size_t m_bytes{0};
    bool m_open{true};
    bool m_sending{false};
};

}

#endif

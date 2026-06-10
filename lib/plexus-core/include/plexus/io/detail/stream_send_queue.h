#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_STREAM_SEND_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_STREAM_SEND_QUEUE_H

#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <limits>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// The STREAM sibling of send_queue: the same sans-IO serial outbound discipline a
// reliable byte stream needs, hoisted out of the asio socket / ssl::stream so the
// plaintext TCP channel and the TLS channel reuse ONE block instead of each
// hand-rolling a near-identical bounded byte-FIFO write loop. The only structural
// difference from send_queue is the absence of an Endpoint: a stream is already
// point-to-point, so the send-sink is async_write(socket/stream, buffer) with no
// destination. The discipline is otherwise byte-identical: enqueue() copies the
// caller's bytes SYNCHRONOUSLY into a block-OWNED node (the async write is a
// non-owning view and does not copy, so a reused caller scratch would corrupt an
// in-flight frame); the queue drains SERIALLY (at most one async_write outstanding,
// its completion pops the front and chains the next); close() drops the queue and a
// completion firing after close is a guarded no-op.
//
// Capacity is a required-WITH-default knob (default = unbounded, a no-cap sentinel):
// under the default the block is byte-identical to an unbounded socket write queue
// and the at-capacity signal is inert. With a finite capacity, enqueue() refuses to
// admit past the cap (returns false; full() observes the state) so a capped channel
// can shed (congestion=drop) or stall (congestion=block) at the bound; admission
// resumes once a drain frees room. The cap accounts the SUMMED PAYLOAD BYTES, not the
// entry count, with a compare-BEFORE-add admission so a crafted large frame cannot
// wrap the running total past the cap and re-admit.
//
// The fail-on-error edge (Pitfall 4): a stream channel FAILS the channel on a socket
// error. The composer's send-sink reports the error and then closes the block; the
// completion's open-guard makes the post-close chaining a no-op, so the next queued
// frame is never written through the failed socket. The error is surfaced to the
// composer, never swallowed by the block. Single-owner, bare `this`, no shared
// lifetime — the owner closes the block before it dies.
class stream_send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    // The send-sink: given the block-owned bytes view, perform the irreducible
    // async_write and invoke the completion with ok once it finishes.
    using completion = plexus::detail::move_only_function<void(bool)>;
    using send_sink = plexus::detail::move_only_function<void(std::span<const std::byte>, completion)>;

    explicit stream_send_queue(send_sink sink, std::size_t byte_cap = unbounded)
        : m_sink(std::move(sink))
        , m_byte_cap(byte_cap)
    {
    }

    // Copy the caller's bytes into an owned node and kick the serial drain if idle.
    // Returns false (admitting nothing) when admitting this frame would carry the queued
    // byte total past the cap — the at-capacity backpressure signal; inert under the
    // unbounded default. Compare-before-add (no wrap).
    bool enqueue(std::span<const std::byte> bytes)
    {
        if(!admits(bytes.size()))
            return false;
        m_bytes += bytes.size();
        m_queue.emplace_back(bytes.begin(), bytes.end());
        if(!m_sending)
            drive();
        return true;
    }

    // True when the queued byte total has reached the cap; always false when unbounded.
    [[nodiscard]] bool full() const noexcept { return m_bytes >= m_byte_cap; }

    [[nodiscard]] std::size_t size() const noexcept { return m_queue.size(); }

    // The summed payload bytes of the queued (not-yet-drained) nodes.
    [[nodiscard]] std::size_t queued_bytes() const noexcept { return m_bytes; }

    [[nodiscard]] bool sending() const noexcept { return m_sending; }

    // Drop the queue; a completion firing after close is a guarded no-op. The channel's
    // fail path closes the block so a failed write does not chain onto a dead socket.
    void close()
    {
        m_open = false;
        m_sending = false;
        m_queue.clear();
        m_bytes = 0;
    }

private:
    // Compare-before-add admission: a frame fits only when the cap is not already met AND
    // its size is within the remaining budget (cap - bytes, no wrap).
    [[nodiscard]] bool admits(std::size_t size) const noexcept
    {
        return m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes;
    }

    // Send the front node, and on completion pop it (freeing a slot under a finite cap)
    // and chain the next. At most one send-sink invocation is outstanding, so a node's
    // bytes stay valid until its own completion runs; the open-guard stops the chain
    // after the composer fails (and closes) the block on a socket error.
    void drive()
    {
        if(m_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending = true;
        m_sink(std::span<const std::byte>{m_queue.front()},
            [this](bool /*ok*/)
            {
                if(!m_open)
                    return;
                m_bytes -= m_queue.front().size();
                m_queue.pop_front();
                drive();
            });
    }

    send_sink m_sink;
    std::size_t m_byte_cap;
    std::deque<std::vector<std::byte>> m_queue;
    std::size_t m_bytes{0};
    bool m_open{true};
    bool m_sending{false};
};

}

#endif

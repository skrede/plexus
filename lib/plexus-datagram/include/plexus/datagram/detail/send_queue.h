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

// A generic, sans-IO serial outbound discipline: the bytes-lifetime + one-in-flight
// drain that every datagram backend needs, hoisted out of the asio socket so a plain
// UDP egress and a future crypto egress reuse the SAME block. enqueue() copies the
// caller's bytes SYNCHRONOUSLY into a block-OWNED node — the backend send-sink is a
// non-owning view and the async op does not copy, so a caller scratch reused on the
// next send would corrupt an in-flight datagram; owning the node closes that hazard.
// The queue drains SERIALLY: at most one send-sink invocation is outstanding, its
// completion pops the front and chains the next; close() drops the queue and a
// completion firing after close is a guarded no-op.
//
// Capacity is a required-WITH-default knob (default = unbounded, a no-cap sentinel):
// under the default the block is byte-identical to an unbounded socket queue and the
// at-capacity signal is inert. When constructed with a finite capacity, enqueue()
// refuses to admit past the cap (returns false; full() observes the state) so a
// capped caller can react to backpressure; admission resumes once a drain frees room.
// This bounded-send surface is the block's OWN capacity, not a window-coupled parking
// queue — that distinct concern stays a separate, channel-side composed member.
//
// The cap accounts the SUMMED PAYLOAD BYTES of the queued nodes, not the entry count:
// a single large frame must trip the budget even though it is one entry (a count cap
// admits a 4 MB frame unboundedly while count stays 1). Admission is compare-BEFORE-add
// against the running total so the sum never integer-overflows past the cap and
// re-admits — a crafted sequence of large frames cannot wrap m_bytes below the cap.
//
// The send-sink reads the block-owned node and signals completion: it is the only
// irreducible backend mechanism (the async write). Single-owner, bare `this`, no
// shared lifetime — the owner closes the block before it dies.
template<typename Endpoint>
class send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    // The send-sink: given the block-owned bytes view + the destination, perform the
    // irreducible async op and invoke the completion with ok once it finishes.
    using completion = plexus::detail::move_only_function<void(bool)>;
    using send_sink  = plexus::detail::move_only_function<void(std::span<const std::byte>, const Endpoint &, completion)>;

    explicit send_queue(send_sink sink, std::size_t byte_cap = unbounded)
            : m_sink(std::move(sink))
            , m_byte_cap(byte_cap)
    {
    }

    // Copy the caller's bytes into an owned node and kick the serial drain if idle.
    // Returns false (admitting nothing) when admitting this frame would carry the queued
    // byte total past the cap — the at-capacity backpressure signal; inert under the
    // unbounded default. The check is compare-before-add (no wrap): a frame is refused
    // when m_bytes already meets the cap OR the frame's size would not fit in the
    // remaining budget, so the running total can never overflow past the cap.
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

    // True when the queued byte total has reached the cap and no further frame is
    // admitted until a drain frees room; always false under the unbounded default.
    [[nodiscard]] bool full() const noexcept
    {
        return m_bytes >= m_byte_cap;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_queue.size();
    }

    // The summed payload bytes of the queued (not-yet-drained) nodes.
    [[nodiscard]] std::size_t queued_bytes() const noexcept
    {
        return m_bytes;
    }

    [[nodiscard]] bool sending() const noexcept
    {
        return m_sending;
    }

    // Drop the queue; a completion firing after close is a guarded no-op. close() is terminal
    // (m_open never re-arms), so the recycled-buffer pool is released here too — it is spare
    // capacity with no reuse left after close, and dropping it returns the memory promptly.
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
        Endpoint               dest;
    };

    // Compare-before-add admission: a frame fits only when the cap is not already met AND
    // its size is within the remaining budget. Phrasing the remaining-budget test as a
    // subtraction (cap - bytes, both unsigned with bytes < cap guaranteed by the first
    // clause) cannot overflow, so no crafted size can wrap the comparison.
    [[nodiscard]] bool admits(std::size_t size) const noexcept
    {
        return m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes;
    }

    // Pull a recycled buffer (reusing its heap capacity) for the bytes, or grow one when
    // the freelist is empty. clear()+assign reuses the spilled capacity, so a burst that
    // re-fills the queue allocates nothing once the freelist has warmed to the burst depth.
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

    // Return a drained node's buffer to the freelist for reuse, dropping it instead when the
    // freelist is already at its node-count bound (so the pool of spare buffers cannot grow
    // without limit even though the byte cap bounds the LIVE queue, not the spare set).
    void recycle_buffer(std::vector<std::byte> &&buf)
    {
        if(m_free_buffers.size() < k_max_free_buffers)
            m_free_buffers.push_back(std::move(buf));
    }

    // Send the front node, and on completion pop it (freeing a slot under a finite cap),
    // recycle its buffer, and chain the next. At most one send-sink invocation is
    // outstanding, so a node's bytes stay valid until its own completion runs.
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

    // The spare-buffer pool's node-count ceiling: a small fixed bound so a momentary deep
    // burst leaves a bounded set of warm buffers behind, never an unbounded spare pool. A
    // burst deeper than this still works (the excess buffers free on drain); it just does not
    // pool every one. The byte cap bounds the LIVE queue; this independently bounds the idle pool.
    static constexpr std::size_t k_max_free_buffers = 64;

    send_sink                           m_sink;
    std::size_t                         m_byte_cap;
    std::deque<node>                    m_queue;
    std::vector<std::vector<std::byte>> m_free_buffers; // recycled buffer pool (capacity reused)
    std::size_t                         m_bytes{0};
    bool                                m_open{true};
    bool                                m_sending{false};
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_SEND_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_SEND_QUEUE_H

#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <limits>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

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
    using send_sink = plexus::detail::move_only_function<void(std::span<const std::byte>, const Endpoint &, completion)>;

    explicit send_queue(send_sink sink, std::size_t capacity = unbounded)
        : m_sink(std::move(sink))
        , m_capacity(capacity)
    {
    }

    // Copy the caller's bytes into an owned node and kick the serial drain if idle.
    // Returns false (admitting nothing) when a finite capacity is already full — the
    // at-capacity backpressure signal; inert under the unbounded default.
    bool enqueue(std::span<const std::byte> bytes, const Endpoint &dest)
    {
        if(m_queue.size() >= m_capacity)
            return false;
        m_queue.push_back(node{std::vector<std::byte>(bytes.begin(), bytes.end()), dest});
        if(!m_sending)
            drive();
        return true;
    }

    // True when a finite capacity is reached and no further node is admitted until a
    // drain frees room; always false under the unbounded default.
    [[nodiscard]] bool full() const noexcept { return m_queue.size() >= m_capacity; }

    [[nodiscard]] std::size_t size() const noexcept { return m_queue.size(); }

    [[nodiscard]] bool sending() const noexcept { return m_sending; }

    // Drop the queue; a completion firing after close is a guarded no-op.
    void close()
    {
        m_open = false;
        m_sending = false;
        m_queue.clear();
    }

private:
    struct node
    {
        std::vector<std::byte> bytes;
        Endpoint dest;
    };

    // Send the front node, and on completion pop it (freeing a slot under a finite cap)
    // and chain the next. At most one send-sink invocation is outstanding, so a node's
    // bytes stay valid until its own completion runs.
    void drive()
    {
        if(m_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending = true;
        const auto &front = m_queue.front();
        m_sink(std::span<const std::byte>{front.bytes}, front.dest,
            [this](bool /*ok*/)
            {
                if(!m_open)
                    return;
                m_queue.pop_front();
                drive();
            });
    }

    send_sink m_sink;
    std::size_t m_capacity;
    std::deque<node> m_queue;
    bool m_open{true};
    bool m_sending{false};
};

}

#endif

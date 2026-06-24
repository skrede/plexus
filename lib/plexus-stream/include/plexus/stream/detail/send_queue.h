#ifndef HPP_GUARD_PLEXUS_STREAM_DETAIL_SEND_QUEUE_H
#define HPP_GUARD_PLEXUS_STREAM_DETAIL_SEND_QUEUE_H

#include "plexus/wire_bytes.h"
#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <limits>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::stream::detail {

// The stream sibling of the datagram send_queue: the sans-IO serial outbound discipline a
// reliable byte stream needs, kept free of the asio socket / ssl::stream so the plaintext TCP,
// TLS, and serial channels reuse ONE block (no Endpoint — a stream is point-to-point, so the
// send-sink is a single async_write with no destination).
//
// A drain turn issues ONE async_write over a buffer SEQUENCE gathering the front-N queued nodes
// (asio lowers a ConstBufferSequence to one writev/WSASend), so N frames cost one syscall. Each
// node holds a wire_bytes OWNER: enqueue(span) copies (a reused caller scratch would corrupt an
// in-flight frame); enqueue(wire_bytes) holds the owner with NO copy (zero-copy plaintext). The
// gathered owners stay resident until the SINGLE completion — an early release is read-after-free.
//
// Capacity is required-WITH-default unbounded; a finite cap bounds ADDITIONAL queued BACKLOG, not
// a single message (an EMPTY queue admits one frame of ANY size — see admits()). A capped channel
// sheds or stalls on the backlog and resumes admission once a drain frees room.
//
// On a socket error the composer's send-sink reports it then closes the block; the completion's
// open-guard makes post-close chaining a no-op, so the next frames are never written through the
// failed socket. Single-owner, bare `this`; the owner closes the block before it dies.
class send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    // The gather count per drain turn: how many queued frames coalesce into one writev.
    // Sized to stay well under the IOV_MAX floor (Linux/macOS guarantee >=1024 iovecs per
    // writev/sendmsg) while deep enough that a steady producer amortizes the per-frame
    // syscall the gather removes. Substantiated by the gather-count sweep recorded with this
    // plan, not fixed by feel.
    static constexpr std::size_t default_gather_limit = 64;

    // The send-sink: given a SEQUENCE of block-owned byte views (the front-N gathered nodes),
    // perform the irreducible single async_write over them and invoke the completion when done.
    using buffer_sequence = std::span<const std::span<const std::byte>>;
    using completion      = plexus::detail::move_only_function<void(bool)>;
    using send_sink       = plexus::detail::move_only_function<void(buffer_sequence, completion)>;

    explicit send_queue(send_sink sink, std::size_t byte_cap = unbounded, std::size_t gather_limit = default_gather_limit)
            : m_sink(std::move(sink))
            , m_byte_cap(byte_cap)
            , m_gather_limit(gather_limit)
    {
    }

    // Copy the caller's bytes into a node-owned buffer and kick the serial drain if idle.
    // The copy is the price of a transient caller view: the band-drain path hands a recycled
    // pool slot, so the node must own its bytes past the slot's reuse. Returns false (admitting
    // nothing) when this frame would carry the queued byte total past the cap — the at-capacity
    // backpressure signal; inert under the unbounded default.
    bool enqueue(std::span<const std::byte> bytes)
    {
        if(!admits(bytes.size()))
            return false;
        auto                       owned = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
        std::span<const std::byte> view{*owned};
        return admit(wire_bytes<>{view, std::move(owned)});
    }

    // Hold the supplied wire_bytes owner and pass its view with NO copy (the zero-copy plaintext
    // path): the owner keeps the bytes alive across the single gather-write completion. Same
    // compare-before-add cap admission as the copying overload.
    bool enqueue(wire_bytes<> frame)
    {
        if(!admits(frame.size()))
            return false;
        return admit(std::move(frame));
    }

    // True when the queued byte total has reached the cap; always false when unbounded.
    [[nodiscard]] bool full() const noexcept
    {
        return m_bytes >= m_byte_cap;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_queue.size();
    }

    [[nodiscard]] std::size_t queued_bytes() const noexcept
    {
        return m_bytes;
    }

    // The configured byte cap (unbounded when uncapped): the bound the egress scheduler's low-
    // water gate tracks so the band hand-off and this queue's admission stay in lockstep.
    [[nodiscard]] std::size_t capacity() const noexcept
    {
        return m_byte_cap;
    }

    [[nodiscard]] bool sending() const noexcept
    {
        return m_sending;
    }

    // Drop the queue; a completion firing after close is a guarded no-op. The channel's fail
    // path closes the block so a failed write does not chain onto a dead socket.
    void close()
    {
        m_open      = false;
        m_sending   = false;
        m_in_flight = 0;
        m_queue.clear();
        m_views.clear();
        m_bytes = 0;
    }

    // Close and report the count of still-queued frames abandoned: a clean close returns 0,
    // a close over a backlog returns the residual count the caller surfaces as loss (under
    // drop_cause::closed_unsent). The abandoned bytes are NOT flushed — a synchronous
    // non-blocking write would bypass the owning stream's TLS layer, and a graceful async
    // drain-with-deadline is out of scope.
    [[nodiscard]] std::size_t close_and_drain() noexcept
    {
        const std::size_t residual = m_queue.size();
        close();
        return residual;
    }

private:
    // An empty queue admits one frame of ANY size: the per-message ceiling already bounds the
    // message upstream at publish, so this cap must never refuse a single within-ceiling message
    // — it only bounds the EXTRA backlog queued behind an in-flight one. Past the first frame,
    // compare-before-add against the remaining budget (no wrap).
    [[nodiscard]] bool admits(std::size_t size) const noexcept
    {
        return m_bytes == 0 || (m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes);
    }

    bool admit(wire_bytes<> frame)
    {
        m_bytes += frame.size();
        m_queue.push_back(std::move(frame));
        if(!m_sending)
            drive();
        return true;
    }

    // Gather the front-N nodes into one buffer sequence and issue a SINGLE async_write; on
    // completion pop exactly those N and chain the next turn (they stay RESIDENT across the
    // write — an early free is read-after-free). The open-guard stops the chain after a
    // failed-and-closed block.
    void drive()
    {
        if(m_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending   = true;
        m_in_flight = std::min(m_gather_limit, m_queue.size());
        m_views.clear();
        for(std::size_t i = 0; i < m_in_flight; ++i)
            m_views.push_back(static_cast<std::span<const std::byte>>(m_queue[i]));
        m_sink(buffer_sequence{m_views},
               [this](bool /*ok*/)
               {
                   if(!m_open)
                       return;
                   for(std::size_t i = 0; i < m_in_flight; ++i)
                   {
                       m_bytes -= m_queue.front().size();
                       m_queue.pop_front();
                   }
                   drive();
               });
    }

    send_sink                               m_sink;
    std::size_t                             m_byte_cap;
    std::size_t                             m_gather_limit;
    std::deque<wire_bytes<>>                m_queue;
    std::vector<std::span<const std::byte>> m_views; // reused gather scratch (grows once)
    std::size_t                             m_in_flight{0};
    std::size_t                             m_bytes{0};
    bool                                    m_open{true};
    bool                                    m_sending{false};
};

}

#endif

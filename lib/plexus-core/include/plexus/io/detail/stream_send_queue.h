#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_STREAM_SEND_QUEUE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_STREAM_SEND_QUEUE_H

#include "plexus/wire_bytes.h"
#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <limits>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// The STREAM sibling of send_queue: the same sans-IO serial outbound discipline a
// reliable byte stream needs, hoisted out of the asio socket / ssl::stream so the
// plaintext TCP channel and the TLS channel reuse ONE block instead of each
// hand-rolling a near-identical bounded byte-FIFO write loop. The only structural
// difference from send_queue is the absence of an Endpoint: a stream is already
// point-to-point, so the send-sink is async_write(socket/stream, buffers) with no
// destination.
//
// A drain turn issues ONE async_write over a buffer SEQUENCE gathering the front-N
// queued nodes (asio lowers a ConstBufferSequence to a single writev/WSASend in the
// composer, never per-frame), so N queued frames cost one syscall instead of N. Each
// node holds a wire_bytes OWNER handle: enqueue(span) copies the caller's bytes into a
// node-owned buffer (the async write is a non-owning view, so a reused caller scratch
// would corrupt an in-flight frame), while enqueue(wire_bytes) holds the supplied owner
// and passes its view with NO copy (the zero-copy plaintext path). Either way the
// gathered owners stay resident in the in-flight set until the SINGLE completion fires —
// the kernel reads all N buffers in the one writev, so releasing any owner before the
// completion would be a read-after-free. close() drops the queue and a completion firing
// after close is a guarded no-op.
//
// Capacity is a required-WITH-default knob (default = unbounded, a no-cap sentinel):
// under the default the block is byte-identical to an unbounded socket write queue
// and the at-capacity signal is inert. With a finite capacity the cap bounds ADDITIONAL
// queued BACKLOG, not the size of a single message: an EMPTY queue always admits one
// frame of ANY size (the negotiated per-message ceiling, enforced upstream at publish,
// is the SOLE authority over message size; this local cap only governs how much extra
// backlog may pile up behind an in-flight message). Past the first frame a finite cap
// refuses to admit beyond the bound (returns false; full() observes the state) so a
// capped channel can shed (congestion=drop) or stall (congestion=block) on the backlog;
// admission resumes once a drain frees room. The cap accounts the SUMMED PAYLOAD BYTES,
// not the entry count, with a compare-BEFORE-add admission so a crafted large frame
// cannot wrap the running total past the cap and re-admit.
//
// The fail-on-error edge (Pitfall 4): a stream channel FAILS the channel on a socket
// error. The composer's send-sink reports the error and then closes the block; the
// completion's open-guard makes the post-close chaining a no-op, so the next queued
// frames are never written through the failed socket. The error is surfaced to the
// composer, never swallowed by the block. Single-owner, bare `this`, no shared
// lifetime — the owner closes the block before it dies.
class stream_send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    // The gather count per drain turn: the bound on how many queued frames coalesce
    // into one writev. Sized so the gathered iovec stays well under the IOV_MAX floor
    // (Linux/macOS guarantee at least 1024 iovecs per writev/sendmsg) while large
    // enough that a steady producer amortizes the syscall over a deep batch; the
    // residual cost the gather removes is the per-frame syscall, so the batch only
    // needs to be deep enough to dominate it. Substantiated by the gather-count sweep
    // recorded with this plan, not fixed by feel.
    static constexpr std::size_t default_gather_limit = 64;

    // The send-sink: given a SEQUENCE of block-owned byte views (the front-N gathered
    // nodes), perform the irreducible single async_write over them and invoke the
    // completion with ok once it finishes.
    using buffer_sequence = std::span<const std::span<const std::byte>>;
    using completion = plexus::detail::move_only_function<void(bool)>;
    using send_sink = plexus::detail::move_only_function<void(buffer_sequence, completion)>;

    explicit stream_send_queue(send_sink sink, std::size_t byte_cap = unbounded,
                               std::size_t gather_limit = default_gather_limit)
        : m_sink(std::move(sink))
        , m_byte_cap(byte_cap)
        , m_gather_limit(gather_limit)
    {
    }

    // Copy the caller's bytes into a node-owned buffer and kick the serial drain if idle.
    // The copy is the price of a transient caller view: the band-drain path hands a
    // recycled pool slot, so the node must own its bytes past the slot's reuse. Returns
    // false (admitting nothing) when admitting this frame would carry the queued byte
    // total past the cap — the at-capacity backpressure signal; inert under the unbounded
    // default. Compare-before-add (no wrap).
    bool enqueue(std::span<const std::byte> bytes)
    {
        if(!admits(bytes.size()))
            return false;
        auto owned = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
        std::span<const std::byte> view{*owned};
        return admit(wire_bytes<>{view, std::move(owned)});
    }

    // Hold the supplied wire_bytes owner and pass its view with NO copy (the zero-copy
    // plaintext path): the owner keeps the bytes alive across the single gather-write
    // completion. Same compare-before-add cap admission as the copying overload.
    bool enqueue(wire_bytes<> frame)
    {
        if(!admits(frame.size()))
            return false;
        return admit(std::move(frame));
    }

    // True when the queued byte total has reached the cap; always false when unbounded.
    [[nodiscard]] bool full() const noexcept { return m_bytes >= m_byte_cap; }

    [[nodiscard]] std::size_t size() const noexcept { return m_queue.size(); }

    // The summed payload bytes of the queued (not-yet-drained) nodes.
    [[nodiscard]] std::size_t queued_bytes() const noexcept { return m_bytes; }

    // The configured byte cap (unbounded when uncapped): the bound the egress scheduler's
    // low-water gate tracks so the band hand-off and this queue's admission stay in lockstep.
    [[nodiscard]] std::size_t capacity() const noexcept { return m_byte_cap; }

    [[nodiscard]] bool sending() const noexcept { return m_sending; }

    // Drop the queue; a completion firing after close is a guarded no-op. The channel's
    // fail path closes the block so a failed write does not chain onto a dead socket.
    void close()
    {
        m_open = false;
        m_sending = false;
        m_in_flight = 0;
        m_queue.clear();
        m_views.clear();
        m_bytes = 0;
    }

    // Close and report the count of still-queued frames abandoned in the process: a clean
    // close returns 0, a close over a backlog returns the residual frame count the caller
    // surfaces as loss (the channel adds it to its drop edge under drop_cause::closed_unsent).
    // The abandoned bytes are NOT flushed — a synchronous non-blocking write would bypass the
    // TLS layer of the owning stream, and a graceful async drain-with-deadline is out of scope.
    [[nodiscard]] std::size_t close_and_drain() noexcept
    {
        const std::size_t residual = m_queue.size();
        close();
        return residual;
    }

private:
    // An empty queue admits one frame of ANY size: the per-message ceiling already bounds
    // the message upstream at publish, so this cap must never refuse a single within-ceiling
    // message — it only bounds the EXTRA backlog queued behind an in-flight one. Past the
    // first frame, compare-before-add against the remaining budget (no wrap).
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

    // Gather the front-N nodes (N bounded by the gather limit) into one buffer sequence
    // and issue a SINGLE async_write; on completion pop exactly those N (freeing slots
    // under a finite cap) and chain the next turn. The gathered nodes stay RESIDENT in
    // m_queue across the in-flight write — the kernel reads all N owners in the one
    // writev, so an owner freed before the completion would be read-after-free. The
    // open-guard stops the chain after the composer fails (and closes) the block.
    void drive()
    {
        if(m_queue.empty())
        {
            m_sending = false;
            return;
        }
        m_sending = true;
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

    send_sink m_sink;
    std::size_t m_byte_cap;
    std::size_t m_gather_limit;
    std::deque<wire_bytes<>> m_queue;
    std::vector<std::span<const std::byte>> m_views;   // reused gather scratch (grows once)
    std::size_t m_in_flight{0};
    std::size_t m_bytes{0};
    bool m_open{true};
    bool m_sending{false};
};

}

#endif

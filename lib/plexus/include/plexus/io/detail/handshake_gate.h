#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_HANDSHAKE_GATE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_HANDSHAKE_GATE_H

#include "plexus/detail/compat.h"

#include <span>
#include <deque>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::io::detail {

// The open-before-data gate: a generic, sans-IO buffer that holds outbound submissions
// until a readiness edge, then drains them in FIFO order. Hoisted out of the crypto
// channel's enqueue-always + drain-on-handshake-done pattern so a backend that must not
// emit application bytes until its handshake completes reuses the SAME block instead of
// re-inlining a write queue + a done flag. The owner installs a drain callback (the
// irreducible backend egress — the real async write); the gate owns the byte nodes.
//
// submit() before ready COPIES the caller's bytes into an OWNED node and buffers it (the
// caller's scratch may be reused immediately, so a non-owning view would corrupt a held
// node — owning the node closes that hazard). mark_ready() drains the buffer in FIFO
// order through the drain, THEN flips ready. submit() after ready forwards straight to
// the drain with no buffering. reset() drops the buffer (teardown).
//
// A backend that prefers the pre-ready DROP variant (a crypto channel that discards
// sends before its handshake completes) simply does not buffer through this gate — the
// canonical behavior here is enqueue-then-drain. Single-owner, no shared lifetime — the
// owner outlives the gate and resets it before it dies.
class handshake_gate
{
public:
    // The drain sink: given a bytes view (a gate-owned node when draining the buffer,
    // or the caller's view on the pass-through after ready), perform the egress.
    using drain_fn = plexus::detail::move_only_function<void(std::span<const std::byte>)>;

    explicit handshake_gate(drain_fn drain)
        : m_drain(std::move(drain))
    {
    }

    // Copy into an owned node and buffer when not ready; forward straight to the drain
    // once ready.
    void submit(std::span<const std::byte> bytes)
    {
        if(!m_ready)
        {
            m_buffer.emplace_back(bytes.begin(), bytes.end());
            return;
        }
        if(m_drain)
            m_drain(bytes);
    }

    // Drain the buffered nodes in FIFO order through the drain, THEN flip ready so a
    // later submit passes straight through. Idempotent: a second mark_ready on an empty
    // buffer is a no-op.
    void mark_ready()
    {
        while(!m_buffer.empty())
        {
            if(m_drain)
                m_drain(std::span<const std::byte>{m_buffer.front()});
            m_buffer.pop_front();
        }
        m_ready = true;
    }

    [[nodiscard]] bool is_ready() const noexcept { return m_ready; }

    [[nodiscard]] std::size_t buffered() const noexcept { return m_buffer.size(); }

    // Drop the buffer (teardown). Leaves the ready flag intact — a torn-down gate is not
    // re-armed here; the owner reconstructs the block for a fresh handshake.
    void reset() { m_buffer.clear(); }

private:
    drain_fn m_drain;
    std::deque<std::vector<std::byte>> m_buffer;
    bool m_ready{false};
};

}

#endif

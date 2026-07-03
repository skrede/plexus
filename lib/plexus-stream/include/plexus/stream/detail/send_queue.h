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

// A sans-IO serial outbound discipline for a reliable byte stream. A drain turn issues one
// async_write over a buffer sequence gathering the front-N queued nodes (asio lowers it to one
// writev/WSASend), so N frames cost one syscall; the gathered owners stay resident until the
// single completion — an early release is read-after-free. A finite cap bounds the additional
// queued backlog, not a single message (an empty queue admits one frame of any size, see admits()).
class send_queue
{
public:
    static constexpr std::size_t unbounded = std::numeric_limits<std::size_t>::max();

    // Stays well under the IOV_MAX floor (Linux/macOS guarantee >=1024 iovecs per writev/sendmsg)
    // while deep enough to amortize the per-frame syscall the gather removes.
    static constexpr std::size_t default_gather_limit = 64;

    using buffer_sequence = std::span<const std::span<const std::byte>>;
    using completion      = plexus::detail::move_only_function<void(bool)>;
    using send_sink       = plexus::detail::move_only_function<void(buffer_sequence, completion)>;

    explicit send_queue(send_sink sink, std::size_t byte_cap = unbounded, std::size_t gather_limit = default_gather_limit)
            : m_sink(std::move(sink))
            , m_byte_cap(byte_cap)
            , m_gather_limit(gather_limit)
    {
    }

    // The node must own its bytes past the caller's reuse, so a transient view is copied; returns
    // false (admitting nothing) when this frame would carry the queued byte total past the cap.
    bool enqueue(std::span<const std::byte> bytes)
    {
        if(!admits(bytes.size()))
            return false;
        auto owned = std::make_shared<std::vector<std::byte>>(bytes.begin(), bytes.end());
        std::span<const std::byte> view{*owned};
        return admit(wire_bytes<>{view, std::move(owned)});
    }

    // Hold the supplied owner and pass its view with no copy; the owner keeps the bytes alive
    // across the single gather-write completion.
    bool enqueue(wire_bytes<> frame)
    {
        if(!admits(frame.size()))
            return false;
        return admit(std::move(frame));
    }

    // Admit a payload and its trailer as ONE atomic unit: both nodes are stored only if their
    // combined size fits the remaining budget (an empty queue admits the pair whole), otherwise
    // neither is stored and the caller sees the full signal — the wire never carries one without
    // the other. The two stored nodes are ordinary queued frames drained by the existing single
    // completion; no additional completion is introduced for the pair.
    bool enqueue_both(wire_bytes<> payload, wire_bytes<> trailer)
    {
        if(!admits(payload.size() + trailer.size()))
            return false;
        stash(std::move(payload));
        stash(std::move(trailer));
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

    std::size_t capacity() const noexcept
    {
        return m_byte_cap;
    }

    bool sending() const noexcept
    {
        return m_sending;
    }

    // A completion firing after close is a guarded no-op, so a failed write does not chain onto a
    // dead socket.
    void close()
    {
        m_open      = false;
        m_sending   = false;
        m_in_flight = 0;
        m_queue.clear();
        m_views.clear();
        m_bytes = 0;
    }

    // Close and report the count of still-queued frames abandoned (surfaced by the caller as loss
    // under drop_cause::closed_unsent); the abandoned bytes are not flushed.
    std::size_t close_and_drain() noexcept
    {
        const std::size_t residual = m_queue.size();
        close();
        return residual;
    }

private:
    // An empty queue admits one frame of any size (the per-message ceiling bounds it upstream);
    // past the first frame, compare-before-add against the remaining budget (no wrap).
    bool admits(std::size_t size) const noexcept
    {
        return m_bytes == 0 || (m_bytes < m_byte_cap && size <= m_byte_cap - m_bytes);
    }

    void stash(wire_bytes<> frame)
    {
        m_bytes += frame.size();
        m_queue.push_back(std::move(frame));
    }

    bool admit(wire_bytes<> frame)
    {
        stash(std::move(frame));
        if(!m_sending)
            drive();
        return true;
    }

    // Gather the front-N nodes into one buffer sequence and issue a single async_write; they stay
    // resident across the write (an early free is read-after-free), and on completion pop exactly
    // those N and chain the next turn.
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

    send_sink m_sink;
    std::size_t m_byte_cap;
    std::size_t m_gather_limit;
    std::deque<wire_bytes<>> m_queue;
    std::vector<std::span<const std::byte>> m_views;
    std::size_t m_in_flight{0};
    std::size_t m_bytes{0};
    bool m_open{true};
    bool m_sending{false};
};

}

#endif

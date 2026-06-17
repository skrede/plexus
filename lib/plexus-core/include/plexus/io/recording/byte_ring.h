#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_RING_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_RING_H

#include "plexus/io/recording/byte_sink.h"

#include "plexus/wire/cursor.h"
#include "plexus/wire/varint.h"

#include <span>
#include <array>
#include <atomic>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace plexus::io::recording {

// The ring overflow discipline. drop_newest refuses an admit that does not fit and
// leaves the unread backlog intact (continuous-drain capture); drop_oldest evicts
// whole unread records until the new one fits (the pre-buffer/FDR overwrite mode).
enum class ring_policy : std::uint8_t
{
    drop_newest = 0,
    drop_oldest = 1,
};

// A bounded, grown-once, single-producer/single-consumer byte ring carrying
// length-prefixed records [varint len][bytes]. The backing store is allocated ONCE
// (an owned vector sized to the byte budget, or a caller-provided span for the MCU
// "statically provided" path) and never reallocates while pushing — the no-hot-
// path-alloc invariant. The producer (the recorder's executor turn) never blocks:
// try_push admits iff the frame fits (drop_newest) or after evicting whole oldest
// records (drop_oldest). head/tail are monotonic absolute byte counters mapped into
// the store by modulo; release on the producer publishes a frame the acquiring
// consumer then reads. The byte budget is a construction parameter and a documented
// PLACEHOLDER — no tuned default is baked here (the empirical sweep is a later
// milestone). Record framing reuses the wire cursor's varint; the index arithmetic
// and the wrapping copy are the only ring-specific logic.
class byte_ring
{
public:
    explicit byte_ring(std::size_t capacity_bytes, ring_policy policy = ring_policy::drop_newest)
        : m_owned(capacity_bytes), m_store(m_owned), m_policy(policy)
    {
    }

    byte_ring(std::span<std::byte> store, ring_policy policy = ring_policy::drop_newest) noexcept
        : m_store(store), m_policy(policy)
    {
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return m_store.size(); }

    [[nodiscard]] std::size_t used() const noexcept
    {
        return static_cast<std::size_t>(m_head.load(std::memory_order_acquire) -
                                        m_tail.load(std::memory_order_acquire));
    }

    // Frame and admit one record. Returns false without blocking or overwriting the
    // unread backlog when the frame cannot fit under drop_newest; under drop_oldest
    // it evicts whole oldest records first and admits unless the frame exceeds the
    // whole capacity (a record larger than the ring can never be held).
    bool try_push(std::span<const std::byte> record) noexcept
    {
        std::array<std::byte, 10> len_buf{};
        wire::writer              len_writer{len_buf};
        len_writer.varint(record.size());
        const std::size_t frame = len_writer.offset() + record.size();
        if(frame > capacity())
            return false;

        if(m_policy == ring_policy::drop_oldest)
            evict_until(frame);
        else if(capacity() - used() < frame)
            return false;

        const std::uint64_t at = m_head.load(std::memory_order_relaxed);
        write_wrapped(at, {len_buf.data(), len_writer.offset()});
        write_wrapped(at + len_writer.offset(), record);
        m_head.store(at + frame, std::memory_order_release);
        return true;
    }

    // The monotonic absolute byte counters. A freeze snapshots these two indices to
    // bound a frozen window over the held records — no buffer copy, no allocation (the
    // pre-buffer/FDR snapshot is exactly {head, tail}). The producer publishes at head;
    // the consumer advances tail; both are absolute (never wrapped) so a snapshot stays
    // meaningful while overwrite continues past it.
    [[nodiscard]] std::uint64_t head() const noexcept { return m_head.load(std::memory_order_acquire); }
    [[nodiscard]] std::uint64_t tail() const noexcept { return m_tail.load(std::memory_order_acquire); }

    // The byte at an absolute index, mapped into the store by modulo — the read a freeze
    // uses to decode the oldest resident record's timestamp from a snapshot's frozen tail.
    [[nodiscard]] std::byte at(std::uint64_t pos) const noexcept
    {
        return m_store[static_cast<std::size_t>(pos % capacity())];
    }

    // Pop complete framed records into the sink up to a byte budget and report
    // whether the ring still holds unread records — the cooperative yield the
    // self-re-posting drain rides. No thread: the caller drives it on its own turn.
    bool drain(byte_sink &sink, std::size_t max_bytes)
    {
        std::size_t moved = 0;
        while(moved < max_bytes)
        {
            std::uint64_t len = 0;
            std::size_t   header = 0;
            if(!peek_length(len, header))
                break;

            const std::uint64_t tail = m_tail.load(std::memory_order_relaxed);
            copy_out(tail, header + len, sink);
            m_tail.store(tail + header + len, std::memory_order_release);
            moved += static_cast<std::size_t>(header + len);
        }
        return used() != 0;
    }

    // Drain only the frozen window: pop complete framed records whose frame ends at or
    // before frozen_head, up to a byte budget, and report whether the window still holds
    // unread records. A continuing overwrite under drop_oldest may have already evicted the
    // oldest part of the snapshot (tail advanced past the frozen tail); that part is simply
    // gone — the window drains what is still resident below the frozen head, never reading
    // past it into records admitted after the freeze.
    bool drain_window(byte_sink &sink, std::uint64_t frozen_head, std::size_t max_bytes)
    {
        std::size_t moved = 0;
        while(moved < max_bytes)
        {
            std::uint64_t len    = 0;
            std::size_t   header = 0;
            const std::uint64_t tail = m_tail.load(std::memory_order_relaxed);
            if(tail >= frozen_head || !peek_length(len, header))
                break;
            if(tail + header + len > frozen_head)
                break;
            copy_out(tail, header + len, sink);
            m_tail.store(tail + header + len, std::memory_order_release);
            moved += static_cast<std::size_t>(header + len);
        }
        return m_tail.load(std::memory_order_acquire) < frozen_head && used() != 0;
    }

private:
    [[nodiscard]] std::byte &slot(std::uint64_t pos) noexcept
    {
        return m_store[static_cast<std::size_t>(pos % capacity())];
    }

    void write_wrapped(std::uint64_t at, std::span<const std::byte> src) noexcept
    {
        for(std::byte b : src)
            slot(at++) = b;
    }

    void evict_until(std::size_t frame) noexcept
    {
        while(capacity() - used() < frame)
        {
            std::uint64_t len = 0;
            std::size_t   header = 0;
            if(!peek_length(len, header))
                break;
            m_tail.store(m_tail.load(std::memory_order_relaxed) + header + len,
                         std::memory_order_release);
        }
    }

    bool peek_length(std::uint64_t &len, std::size_t &header) noexcept
    {
        if(used() == 0)
            return false;
        std::array<std::byte, 10> hdr{};
        const std::uint64_t       tail  = m_tail.load(std::memory_order_relaxed);
        const std::size_t         avail = std::min<std::size_t>(hdr.size(), used());
        for(std::size_t i = 0; i < avail; ++i)
            hdr[i] = slot(tail + i);
        std::size_t off = 0;
        auto        v   = wire::read_varint({hdr.data(), avail}, off);
        if(!v)
            return false;
        len    = *v;
        header = off;
        return true;
    }

    void copy_out(std::uint64_t at, std::uint64_t n, byte_sink &sink)
    {
        const std::size_t first = static_cast<std::size_t>(at % capacity());
        const std::size_t run   = std::min<std::size_t>(static_cast<std::size_t>(n), capacity() - first);
        sink.write({m_store.data() + first, run});
        if(run < n)
            sink.write({m_store.data(), static_cast<std::size_t>(n) - run});
    }

    std::vector<std::byte>     m_owned;
    std::span<std::byte>       m_store;
    ring_policy                m_policy;
    std::atomic<std::uint64_t> m_head{0};
    std::atomic<std::uint64_t> m_tail{0};
};

}

#endif

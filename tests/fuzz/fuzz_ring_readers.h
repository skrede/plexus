#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_READERS_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_READERS_H

#include "fuzz_sink.h"
#include "fuzz_region.h"

#include "support/noop_notifier.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/broadcast_ring.h"

#include <atomic>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <numeric>
#include <optional>

namespace fuzz_ring {

using plexus::shm::loan_status;
using plexus::shm::shm_channel;
using plexus::shm::broadcast_ring;

struct reader
{
    std::uint32_t index;
    std::uint32_t generation;
    std::uint64_t position;
};

class ring_readers
{
public:
    ring_readers(fuzz_input &in, broadcast_ring &ring, shm_channel<plexus::testing::noop_notifier> &channel) noexcept
            : m_in(in)
            , m_ring(ring)
            , m_readers()
            , m_released()
            , m_channel(channel)
    {
    }

    // A held reader is claimed and never published, as a lane's receive companion is until its placement or resume.
    void register_reader()
    {
        std::uint32_t index                           = 0;
        const std::optional<std::uint32_t> generation = m_ring.register_cursor(index);
        if(!generation)
            return;
        m_readers.push_back({index, *generation, m_ring.tail_position()});
        if((m_in.u8() & 1) != 0)
            m_ring.publish_cursor(index, m_readers.back().position);
    }

    void unregister_reader()
    {
        if(m_readers.empty())
            return;
        const auto at = m_readers.begin() + static_cast<std::ptrdiff_t>(m_in.u8() % m_readers.size());
        m_ring.unregister_cursor(at->index);
        m_released.push_back(*at);
        m_readers.erase(at);
    }

    void consume()
    {
        if(m_readers.empty())
            return;
        reader &r = m_readers[m_in.u8() % m_readers.size()];
        broadcast_ring::consume_result got;
        const loan_status st = m_ring.consume(r.position, got);
        if(st == loan_status::ok && m_ring.pin_if_current(r.position))
        {
            fuzz_consume(std::accumulate(got.slab.begin(), got.slab.end(), std::byte{0}, [](std::byte a, std::byte b) { return a ^ b; }));
            m_ring.refcount_at(r.position).fetch_sub(1, std::memory_order_seq_cst);
        }
        r.position = st == loan_status::lagged ? got.position : st == loan_status::empty ? r.position : r.position + 1;
        m_ring.publish_cursor(r.index, r.position);
    }

    // A live reader's own claim, a released one, or any slot and generation, as a peer's ready may name.
    void place()
    {
        const std::uint8_t pick = m_in.u8() % 3;
        if(pick == 0 && !m_readers.empty())
            return place_own(m_readers[m_in.u8() % m_readers.size()]);
        if(pick == 1 && !m_released.empty())
            return place_released(m_released[m_in.u8() % m_released.size()]);
        fuzz_consume(placed({static_cast<std::uint32_t>(m_in.any()), static_cast<std::uint32_t>(m_in.any()), 0}));
    }

private:
    fuzz_input &m_in;
    broadcast_ring &m_ring;
    std::vector<reader> m_readers;
    std::vector<reader> m_released;
    shm_channel<plexus::testing::noop_notifier> &m_channel;

    // A held reader's own claim lands, and the reader then reads from where the placement started it.
    void place_own(reader &r)
    {
        if(placed(r))
            r.position = word_of(r.index);
    }

    // A released claim never lands on its slot once that slot is claimed again under another generation.
    void place_released(const reader &stale)
    {
        const std::optional<std::uint32_t> again = (m_in.u8() & 1) != 0 ? m_ring.claim_cursor(stale.index) : std::nullopt;
        if(again)
            m_readers.push_back({stale.index, *again, m_ring.tail_position()});
        const bool landed = placed(stale);
        if(again && landed && *again != stale.generation)
            std::abort();
    }

    // A placement lands only on the word its own claim left waiting.
    bool placed(const reader &r)
    {
        const std::uint64_t before = word_of(r.index);
        const bool landed          = m_channel.place_reader(r.index, r.generation);
        if(landed && before != plexus::shm::cursor_waiting(r.generation))
            std::abort();
        return landed;
    }

    std::uint64_t word_of(std::uint32_t index) noexcept
    {
        return index < plexus::shm::k_max_consumers ? m_ring.header().cursors[index].position.load(std::memory_order_acquire) : 0;
    }
};

}

#endif

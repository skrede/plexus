#ifndef HPP_GUARD_PLEXUS_SHM_SLOT_SUBSCRIBER_H
#define HPP_GUARD_PLEXUS_SHM_SLOT_SUBSCRIBER_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/cpu_relax.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/taken_message.h"

#include "plexus/shm/detail/shm_ring_mapping.h"

#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::shm {

// The consumer over one broadcast ring, owning its OWN in-region read cursor (registered
// at the producer's tail so it sees only messages published after it joins, unregistered
// at destruction). A registration failure (the ring is at its k_max_consumers bound)
// leaves it with no cursor and every take() no-ops to empty. take() resolves a contention
// lap, a best_effort skip-tombstone, and a full-ring lap-behind (jumping to the producer
// tail in one O(1) step) internally, so the caller only ever sees ok/empty. The pin is
// Dekker-safe: take() pins via the ring's pin_if_current and hands that ALREADY-HELD pin
// to the taken_message via its adopt_pin ctor (it does NOT pin a second time), beside a
// copy of the ring's mapping pin.
class slot_subscriber
{
public:
    // The conservative spin-then-park budget, confirmed by a {0,64,256,1k,4k,16k} x rate x
    // payload sweep: 256 reclaims ~-50% P50 at 100kHz on
    // the back-to-back path over pure-park at a sub-1us spin window, never busy-spins idle.
    static constexpr std::uint32_t default_spin_budget = 256;

    struct adopt_cursor_t
    {
    };
    static constexpr adopt_cursor_t adopt_cursor{};

    struct hold_cursor_t
    {
    };
    static constexpr hold_cursor_t hold_cursor{};

    explicit slot_subscriber(broadcast_ring &ring, std::uint32_t spin_budget = default_spin_budget) noexcept
            : m_ring(ring)
            , m_spin_budget(spin_budget)
            , m_cursor_index(0)
            , m_generation(0)
            , m_cursor(0)
            , m_registered(false)
            , m_mapping()
    {
        if(const std::optional<std::uint32_t> generation = m_ring.register_cursor(m_cursor_index))
        {
            m_registered = true;
            m_generation = *generation;
            m_cursor     = m_ring.tail_position();
            m_ring.publish_cursor(m_cursor_index, m_cursor);
        }
    }

    // Takes over a cursor index the caller has already claimed on the ring, gating from the producer's tail.
    slot_subscriber(broadcast_ring &ring, adopt_cursor_t, std::uint32_t cursor_index, std::uint32_t generation, std::uint32_t spin_budget = default_spin_budget,
                    detail::mapping_pin mapping = {}) noexcept
            : slot_subscriber(ring, hold_cursor, cursor_index, generation, spin_budget, std::move(mapping))
    {
        m_ring.publish_cursor(m_cursor_index, m_cursor);
    }

    // Takes over a claimed cursor index that gates nothing until the producer places it or resume starts it.
    slot_subscriber(broadcast_ring &ring, hold_cursor_t, std::uint32_t cursor_index, std::uint32_t generation, std::uint32_t spin_budget = default_spin_budget,
                    detail::mapping_pin mapping = {}) noexcept
            : m_ring(ring)
            , m_spin_budget(spin_budget)
            , m_cursor_index(cursor_index)
            , m_generation(generation)
            , m_cursor(ring.tail_position())
            , m_registered(true)
            , m_mapping(std::move(mapping))
    {
    }

    // Reads from where the producer placed the cursor, else from where it attached; a take that finds that position
    // overwritten jumps to the producer's tail.
    void resume() noexcept
    {
        if(const std::uint64_t placed = m_ring.header().cursors[m_cursor_index].position.load(std::memory_order_acquire); placed != k_cursor_idle)
            m_cursor = placed;
        m_ring.publish_cursor(m_cursor_index, m_cursor);
    }

    ~slot_subscriber()
    {
        if(m_registered)
            m_ring.unregister_cursor(m_cursor_index);
    }

    slot_subscriber(const slot_subscriber &)            = delete;
    slot_subscriber &operator=(const slot_subscriber &) = delete;
    slot_subscriber(slot_subscriber &&)                 = delete;
    slot_subscriber &operator=(slot_subscriber &&)      = delete;

    loan_status take(taken_message &out) noexcept
    {
        if(!m_registered)
            return loan_status::empty;

        std::uint32_t spun = 0;
        for(;;)
        {
            broadcast_ring::consume_result consumed;
            const loan_status st = m_ring.consume(m_cursor, consumed);
            if(st == loan_status::empty)
            {
                if(spin_or_give_up(spun))
                    return loan_status::empty;
                continue;
            }
            loan_status settled;
            if(resolve(st, consumed, out, settled))
                return settled;
        }
    }

    bool registered() const noexcept
    {
        return m_registered;
    }
    std::uint64_t cursor() const noexcept
    {
        return m_cursor;
    }

    std::optional<std::uint32_t> cursor_index() const noexcept
    {
        if(!m_registered)
            return std::nullopt;
        return m_cursor_index;
    }

    std::uint32_t cursor_generation() const noexcept
    {
        return m_generation;
    }

private:
    // Spin (relaxing the core) within the budget so a back-to-back arrival is caught before
    // reporting empty; true past the budget hands off to the backend futex park (~0% CPU idle).
    bool spin_or_give_up(std::uint32_t &spun) noexcept
    {
        if(spun++ >= m_spin_budget)
            return true;
        cpu_relax();
        return false;
    }

    // lagged jumps the cursor to the producer tail; congested steps forward; ok pins the slot
    // Dekker-safe before advancing, retrying on a lost overwrite race rather than aliasing a
    // torn read. Returns true only on a delivered slot; false means retry the loop.
    bool resolve(loan_status st, const broadcast_ring::consume_result &consumed, taken_message &out, loan_status &settled) noexcept
    {
        if(st == loan_status::lagged)
        {
            m_cursor = consumed.position;
            m_ring.publish_cursor(m_cursor_index, m_cursor);
            return false;
        }
        if(st == loan_status::congested || !m_ring.pin_if_current(m_cursor))
        {
            advance();
            return false;
        }
        out = taken_message(taken_message::adopt_pin, consumed.slab.data(), consumed.slab.size(), &m_ring.refcount_at(m_cursor), m_mapping);
        advance();
        settled = loan_status::ok;
        return true;
    }

    void advance() noexcept
    {
        ++m_cursor;
        m_ring.publish_cursor(m_cursor_index, m_cursor);
    }

    broadcast_ring &m_ring;
    std::uint32_t m_spin_budget;
    std::uint32_t m_cursor_index;
    std::uint32_t m_generation;
    std::uint64_t m_cursor;
    bool m_registered;
    detail::mapping_pin m_mapping;
};

}

#endif

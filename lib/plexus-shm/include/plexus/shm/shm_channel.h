#ifndef HPP_GUARD_PLEXUS_SHM_SHM_CHANNEL_H
#define HPP_GUARD_PLEXUS_SHM_SHM_CHANNEL_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/notifier_concept.h"
#include "plexus/shm/ring_cursors.h"
#include "plexus/shm/slot_publisher.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/taken_message.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <thread>
#include <utility>

namespace plexus::shm {

// The send/drain facade over ONE ring and the notifier seam. Its publisher half memcpys each send once into the slab and
// signals only on a successful commit; its subscriber half, engaged only for a holder that reads, takes a reader cursor
// and hands each pending message up zero-copy, reclaiming the pin between deliveries.
template<typename Notifier>
    requires notifier<Notifier>
class shm_channel
{
public:
    using deliver_fn      = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;
    using cursor_alive_fn = plexus::detail::move_only_function<bool(std::uint32_t)>;

    // spin_budget is the consumer-sovereign adaptive spin-then-park knob (required-WITH-default, the swept knee): an
    // engaged subscriber spins up to the budget on an empty take before a drain returns and the notifier parks.
    shm_channel(broadcast_ring &ring, Notifier &notify, io::reliability rel, io::congestion cong, std::uint32_t spin_budget = slot_subscriber::default_spin_budget) noexcept
            : m_notifier(notify)
            , m_ring(ring)
            , m_publisher(ring, rel, cong)
            , m_spin_budget(spin_budget)
            , m_subscriber()
            , m_cursor_alive_cb()
    {
    }

    shm_channel(const shm_channel &)            = delete;
    shm_channel &operator=(const shm_channel &) = delete;
    shm_channel(shm_channel &&)                 = delete;
    shm_channel &operator=(shm_channel &&)      = delete;

    // The notifier fires ONLY on ok.
    loan_status send(std::span<const std::byte> payload) noexcept
    {
        loaned_buffer slot;
        const loan_status loaned = loan_blocking(payload.size(), slot);
        if(loaned != loan_status::ok)
            return loaned;

        std::memcpy(slot.bytes().data(), payload.data(), payload.size());
        slot.set_filled(payload.size());

        const loan_status committed = m_publisher.publish(std::move(slot));
        if(committed != loan_status::ok)
            return committed;

        m_notifier.signal();
        return loan_status::ok;
    }

    void drain(deliver_fn &deliver)
    {
        if(!m_subscriber)
            return;
        for(;;)
        {
            taken_message msg;
            if(m_subscriber->take(msg) != loan_status::ok)
                return;
            deliver(msg.as_wire_bytes());
        }
    }

    // Registers the reader cursor at the producer's tail; nullopt when the ring has no free reader slot.
    std::optional<std::uint32_t> engage_subscriber() noexcept
    {
        if(!m_subscriber)
            m_subscriber.emplace(m_ring, m_spin_budget);
        return m_subscriber->cursor_index();
    }

    // mode is slot_subscriber::adopt_cursor or slot_subscriber::hold_cursor.
    template<typename Mode>
    void engage_subscriber(Mode mode, std::uint32_t claimed_cursor) noexcept
    {
        m_subscriber.emplace(m_ring, mode, claimed_cursor, m_spin_budget);
    }

    void resume_subscriber() noexcept
    {
        if(m_subscriber)
            m_subscriber->resume();
    }

    // The reader in slot cursor starts at the newest message this producer wrote, which the current publish may already
    // be; the switch marker skips the ones before it.
    bool place_reader(std::uint32_t cursor) noexcept
    {
        const std::uint64_t tail = m_ring.tail_position();
        return detail::place_cursor(m_ring.header(), m_ring.header().consumer_capacity, cursor, tail == 0 ? 0 : tail - 1);
    }

    void release_subscriber() noexcept
    {
        m_subscriber.reset();
    }

    std::optional<std::uint32_t> waiter_slot() const noexcept
    {
        if(!m_subscriber)
            return std::nullopt;
        return m_subscriber->cursor_index();
    }

    // Unbound, a congested reliable send reclaims no cursor.
    void bind_cursor_liveness(cursor_alive_fn alive) noexcept
    {
        m_cursor_alive_cb = std::move(alive);
    }

private:
    // The cap on CONSECUTIVE no-progress yield turns before a still-congested reliable+block
    // loan surfaces congested. Gated on consumer PROGRESS, not a fixed turn count: every time
    // the slowest cursor advances the consumer freed a slot, so blocking stays LOSSLESS for any
    // live-but-lagging consumer; the budget only elapses across a whole window of NO cursor
    // motion (a wedged/dead peer), surfacing congested observably rather than hanging.
    static constexpr int k_no_progress_budget = 1 << 16;

    Notifier &m_notifier;
    broadcast_ring &m_ring;
    slot_publisher m_publisher;
    std::uint32_t m_spin_budget;
    std::optional<slot_subscriber> m_subscriber;
    cursor_alive_fn m_cursor_alive_cb;

    // Cursor liveness is probed only once a reliable blocking loan is congested: right away, then after each whole
    // no-progress window, until a pass reclaims nothing.
    loan_status loan_blocking(std::size_t size, loaned_buffer &out) noexcept
    {
        loan_status st = m_publisher.loan(size, out);
        if(st != loan_status::congested || m_publisher.delivery() != io::reliability::reliable || m_publisher.overflow() != io::congestion::block)
            return st;
        reclaim_dead_cursors();
        do
            st = loan_until_stalled(size, out);
        while(st == loan_status::congested && reclaim_dead_cursors());
        return st;
    }

    loan_status loan_until_stalled(std::size_t size, loaned_buffer &out) noexcept
    {
        loan_status st          = m_publisher.loan(size, out);
        std::uint64_t last_seen = m_publisher.slowest_consumer_position();
        for(int stalled = 0; st == loan_status::congested && stalled < k_no_progress_budget; ++stalled)
        {
            std::this_thread::yield();
            st = m_publisher.loan(size, out);

            const std::uint64_t now = m_publisher.slowest_consumer_position();
            if(now != last_seen)
            {
                last_seen = now;
                stalled   = -1; // a live consumer freed a slot: reset the window (++ returns it to 0)
            }
        }
        return st;
    }

    // The channel's own cursor is skipped: its slot is held through this process's own region handle.
    bool reclaim_dead_cursors() noexcept
    {
        if(!m_cursor_alive_cb)
            return false;
        const std::optional<std::uint32_t> own = waiter_slot();
        bool reclaimed                         = false;
        for(std::uint32_t i = 0; i < m_ring.registered_high_water(); ++i)
            if(own != i && m_ring.reclaim_cursor(i, m_cursor_alive_cb))
                reclaimed = true;
        return reclaimed;
    }
};

}

#endif

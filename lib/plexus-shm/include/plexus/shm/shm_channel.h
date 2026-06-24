#ifndef HPP_GUARD_PLEXUS_SHM_SHM_CHANNEL_H
#define HPP_GUARD_PLEXUS_SHM_SHM_CHANNEL_H

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/notifier_concept.h"
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
#include <span>
#include <thread>
#include <utility>

namespace plexus::shm {

// The send/drain facade composing a slot_publisher + slot_subscriber over ONE ring with
// the notifier seam: send memcpys once into the slab then wakes the cross-process consumer
// (signaled ONLY on a successful commit, never on a reject or a congested send); drain hands
// each pending message up zero-copy, the taken_message reclaiming each turn so the pin is
// released between deliveries.
template<typename Notifier>
    requires notifier<Notifier>
class shm_channel
{
public:
    using deliver_fn = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    // spin_budget is the consumer-sovereign adaptive spin-then-park knob threaded to the
    // slot_subscriber (required-WITH-default — the swept knee): the subscriber spins up to
    // the budget on an empty take before this drain returns and the notifier parks.
    shm_channel(broadcast_ring &ring, Notifier &notify, io::reliability rel, io::congestion cong, std::uint32_t spin_budget = slot_subscriber::default_spin_budget) noexcept
            : m_publisher(ring, rel, cong)
            , m_subscriber(ring, spin_budget)
            , m_notifier(notify)
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
        for(;;)
        {
            taken_message msg;
            if(m_subscriber.take(msg) != loan_status::ok)
                return;
            deliver(msg.as_wire_bytes());
        }
    }

private:
    // The cap on CONSECUTIVE no-progress yield turns before a still-congested reliable+block
    // loan surfaces congested. Gated on consumer PROGRESS, not a fixed turn count: every time
    // the slowest cursor advances the consumer freed a slot, so blocking stays LOSSLESS for any
    // live-but-lagging consumer; the budget only elapses across a whole window of NO cursor
    // motion (a wedged/dead peer), surfacing congested observably rather than hanging.
    static constexpr int k_no_progress_budget = 1 << 16;

    loan_status loan_blocking(std::size_t size, loaned_buffer &out) noexcept
    {
        loan_status st = m_publisher.loan(size, out);
        if(m_publisher.delivery() != io::reliability::reliable || m_publisher.overflow() != io::congestion::block)
            return st;

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

    slot_publisher m_publisher;
    slot_subscriber m_subscriber;
    Notifier &m_notifier;
};

}

#endif

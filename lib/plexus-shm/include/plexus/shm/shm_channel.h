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

// The send/drain facade composing a slot_publisher + slot_subscriber over ONE
// ring with the notifier seam. It is the transport's working surface: a send does
// exactly one memcpy into the slab slot then wakes the cross-process consumer; a
// drain hands each pending message up zero-copy.
//
//   send(payload)     loan a slot for payload.size() -> ONE memcpy into the slab
//                     -> publish by move -> signal the notifier. Returns the
//                     loan/publish status: ok on a committed+signaled send;
//                     rejected when payload exceeds the slot capacity (the
//                     oversize fallback -- NO publish, NO signal, NOT a silent
//                     drop); congested when the policy gate blocks (OBSERVABLE via
//                     the return). The notifier is signaled ONLY on a
//                     successful commit -- never on a reject or a congested send.
//   drain(deliver)    take every pending message and hand each up as a zero-copy
//                     wire_bytes<shm_slot_owner> (the slot pinned for the view's
//                     lifetime); the taken_message reclaims at the end of each
//                     iteration, so the pin is released each turn. deliver is a
//                     move_only_function (the project move-only-callback
//                     convention, never a copyable wrapper; core stays asio-free).
//
// The notifier is the seam reference (a type satisfying the core `notifier`
// concept -- the compiled futex primitive or the asio reactor bridge), NOT an asio
// type: the channel never pulls an asio or POSIX header into core. Borrows the ring + notifier
// BY REFERENCE; non-copy/non-move owning facade.
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

    // Loan -> one memcpy -> publish -> signal. The notifier fires ONLY on ok.
    loan_status send(std::span<const std::byte> payload) noexcept
    {
        loaned_buffer     slot;
        const loan_status loaned = loan_blocking(payload.size(), slot);
        if(loaned != loan_status::ok)
            return loaned; // rejected (oversize) or congested: no signal either way

        std::memcpy(slot.bytes().data(), payload.data(), payload.size());
        slot.set_filled(payload.size());

        const loan_status committed = m_publisher.publish(std::move(slot));
        if(committed != loan_status::ok)
            return committed; // a commit-time reject never signals

        m_notifier.signal(); // wake the cross-process consumer -- success only
        return loan_status::ok;
    }

    // Hands every pending message up zero-copy. Each taken_message lives only for
    // its deliver() call, so its pin is released before the next take.
    void drain(deliver_fn &deliver)
    {
        for(;;)
        {
            taken_message msg;
            if(m_subscriber.take(msg) != loan_status::ok)
                return; // empty: nothing more for this cursor
            deliver(msg.as_wire_bytes());
        }
    }

private:
    // PROGRESS-gated blocking bound for a reliable+block producer: the cap on
    // CONSECUTIVE no-progress yield turns (turns during which the slowest consumer
    // cursor did not advance) before a still-congested loan surfaces congested. A
    // reliable producer BLOCKS on the slowest registered cursor (it must not
    // overwrite an unconsumed value) -- so it retries the gated claim, yielding the
    // CPU between turns, until the consumer drains. The loop allocates NOTHING (no
    // kernel object, no heap -- only atomic cursor loads) -- the determinism the
    // safety/drone use needs. The bound is gated on consumer PROGRESS, NOT on a
    // fixed turn count: every time the slowest cursor advances the consumer freed a
    // slot, so blocking stays LOSSLESS for any live-but-lagging consumer however slow;
    // the budget only elapses when the cursor makes NO progress across a whole window,
    // i.e. a genuinely wedged or dead peer -- which then surfaces congested OBSERVABLY,
    // never an infinite hang. best_effort never blocks -- it overwrites the latest and
    // its own gate returns congested only on a full-lap-pinned ring.
    static constexpr int k_no_progress_budget = 1 << 16;

    loan_status loan_blocking(std::size_t size, loaned_buffer &out) noexcept
    {
        loan_status st = m_publisher.loan(size, out);
        if(m_publisher.delivery() != io::reliability::reliable || m_publisher.overflow() != io::congestion::block)
            return st; // best_effort / non-block: surface the status as-is (no blocking)

        std::uint64_t last_seen = m_publisher.slowest_consumer_position();
        for(int stalled = 0; st == loan_status::congested && stalled < k_no_progress_budget; ++stalled)
        {
            std::this_thread::yield(); // allocation-free back-pressure spin
            st = m_publisher.loan(size, out);

            const std::uint64_t now = m_publisher.slowest_consumer_position();
            if(now != last_seen)
            {
                last_seen = now; // a live consumer freed a slot: keep blocking losslessly
                stalled   = -1;  // reset the no-progress window (++ on loop returns it to 0)
            }
        }
        return st;
    }

    slot_publisher  m_publisher;
    slot_subscriber m_subscriber;
    Notifier       &m_notifier;
};

}

#endif

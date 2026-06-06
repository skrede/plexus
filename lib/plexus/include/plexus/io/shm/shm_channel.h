#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_CHANNEL_H

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/loaned_buffer.h"
#include "plexus/io/shm/notifier_concept.h"
#include "plexus/io/shm/slot_publisher.h"
#include "plexus/io/shm/slot_subscriber.h"
#include "plexus/io/shm/taken_message.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <cstddef>
#include <cstring>
#include <span>
#include <utility>

namespace plexus::io::shm {

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
// type: the channel never pulls asio/POSIX into core. Borrows the ring + notifier
// BY REFERENCE; non-copy/non-move owning facade.
template <typename Notifier>
    requires notifier<Notifier>
class shm_channel
{
public:
    using deliver_fn = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    shm_channel(broadcast_ring &ring, Notifier &notify, reliability rel, congestion cong) noexcept
        : m_publisher(ring, rel, cong), m_subscriber(ring), m_notifier(notify)
    {
    }

    shm_channel(const shm_channel &) = delete;
    shm_channel &operator=(const shm_channel &) = delete;
    shm_channel(shm_channel &&) = delete;
    shm_channel &operator=(shm_channel &&) = delete;

    // Loan -> one memcpy -> publish -> signal. The notifier fires ONLY on ok.
    loan_status send(std::span<const std::byte> payload) noexcept
    {
        loaned_buffer slot;
        const loan_status loaned = m_publisher.loan(payload.size(), slot);
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
    slot_publisher  m_publisher;
    slot_subscriber m_subscriber;
    Notifier       &m_notifier;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_LIFETIME_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_LIFETIME_H

#include "plexus/shm/detail/shm_ring_names.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_attach.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/upgrade_channel.h"

#include <atomic>
#include <memory>
#include <string>
#include <cstdint>

namespace plexus::shm::detail {

// A producer holds the creator slot before it stamps or adopts a ring. A name is removed only by a holder of a retired
// ring's creator slot or of every slot, once a fresh attach of the name reads that ring's identity; a holder that
// retires a ring it never stamped first gives it one. The last local holder retires the ring only once it holds every
// slot in one step, and a joiner or a returning producer re-reads the magic after taking its own slot.
struct ring_lifetime
{
    static bool retired(broadcast_ring &ring) noexcept
    {
        return ring.header().magic.load(std::memory_order_acquire) == k_ring_retired;
    }

    // The last local holder's teardown: the consumer side first, then the ring is retired if no holder is left anywhere.
    template<typename Broker, typename Entry>
    static void close(Broker &broker, Entry &e)
    {
        e.teardown();
        drop_creator(e);
        if(e.control.hold_all_slots() != slot_hold::held)
            return;
        if(stamp_retired(e, k_ring_magic))
            ring_names::remove_names(broker, e, e.ctrl_name);
        e.control.release_all_slots();
    }

    template<typename Entry>
    static void drop_creator(Entry &e) noexcept
    {
        if(e.creator)
            e.control.release_slot(k_creator_slot);
        e.creator = false;
    }

    // A ring its creator retired under this reader is lost to it, reported once like a lost waiter. The report may release
    // the entry, so nothing touches it afterwards.
    template<typename Entry, typename Report>
    static void drain(Entry &e, Report &report)
    {
        e.deliver_pending();
        if(!retired(e.ring) || e.loss->status() != arm_status::armed)
            return;
        const std::shared_ptr<waiter_loss> loss = e.loss;
        if(report)
            report(io::fallback_reason::waiter_lost);
        loss->report(arm_status::exhausted);
    }

    // A producer that returns to an entry its consumers kept takes the creator slot again. Only a holder of the creator
    // slot or of every slot moves the magic off k_ring_magic, so the magic read under the slot shows the ring still live.
    template<typename Entry>
    static acquire_bound retake_creator(Entry &e)
    {
        if(e.creator)
            return acquire_bound::none;
        if(const acquire_bound held = take_creator(e); held != acquire_bound::none)
            return held;
        if(ring_names::magic_of(e.control.bytes()) != k_ring_magic)
        {
            drop_creator(e);
            return acquire_bound::transient_join;
        }
        repair(e.ring);
        return acquire_bound::none;
    }

    // Removes the names of a retired ring whose holder died before removing them. Holding every slot shuts out every other
    // remover.
    template<typename Broker, typename Entry>
    static bool remove_retired(Broker &broker, Entry &e, const std::string &ctrl_name)
    {
        if(e.control.hold_all_slots() != slot_hold::held)
            return false;
        const bool same = ring_names::remove_names(broker, e, ctrl_name);
        e.control.release_all_slots();
        e.creator = false;
        return same;
    }

    // A ring retired before its stamp is first given an identity, which the release on the retired stamp publishes.
    template<typename Entry>
    static bool stamp_retired(Entry &e, std::uint64_t found)
    {
        control_header_t *header = ring_names::header_of(e.control.bytes());
        if(header == nullptr)
            return false;
        if(found == 0)
            give_identity(*header);
        return header->magic.compare_exchange_strong(found, k_ring_retired, std::memory_order_release, std::memory_order_relaxed);
    }

    static void give_identity(control_header_t &header)
    {
        std::uint64_t none = 0;
        if(header.magic.load(std::memory_order_acquire) == 0 && header.identity.load(std::memory_order_relaxed) == 0)
            header.identity.compare_exchange_strong(none, draw_ring_identity(), std::memory_order_relaxed);
    }

    template<typename Entry>
    static acquire_bound take_creator(Entry &e)
    {
        switch(e.control.hold_slot(k_creator_slot))
        {
            case slot_hold::held:
                e.creator = true;
                return acquire_bound::none;
            case slot_hold::busy:
                return acquire_bound::creator_alive;
            default:
                return acquire_bound::liveness;
        }
    }

    // A reliable claim recycles a cell only once its occupant committed, so a position a dead creator won and never
    // committed would stop the ring; each such position of the last lap reads empty and becomes a skip.
    static void repair(broadcast_ring &ring) noexcept
    {
        const std::uint64_t tail = ring.tail_position();
        broadcast_ring::consume_result probe;
        for(std::uint64_t p = tail > ring.cell_count() ? tail - ring.cell_count() : 0; p < tail; ++p)
            if(ring.consume(p, probe) == loan_status::empty)
                ring.abandon(p);
    }
};

}

#endif

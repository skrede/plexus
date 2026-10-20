#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_LIFETIME_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_LIFETIME_H

#include "plexus/shm/arm_status.h"
#include "plexus/shm/loan_status.h"
#include "plexus/shm/ring_header.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/upgrade_channel.h"

#include <new>
#include <span>
#include <atomic>
#include <memory>
#include <cstddef>
#include <cstdint>

namespace plexus::shm::detail {

// A producer holds the creator slot before it stamps or adopts a ring. A name is removed only by the creator slot's
// holder right after it stores k_ring_retired over the stamp it found, so the name still belongs to that ring. The last
// local holder retires the ring only once it holds every slot in one step, and a joiner re-reads the magic after taking
// its own slot.
struct ring_lifetime
{
    static bool retired(broadcast_ring &ring) noexcept
    {
        return ring.header().magic.load(std::memory_order_acquire) == k_ring_retired;
    }

    // The last local holder's teardown: the consumer side first, then the ring is retired if no holder is left anywhere.
    template<typename Entry>
    static void close(Entry &e)
    {
        e.teardown();
        drop_creator(e);
        if(e.control.hold_all_slots() != slot_hold::held)
            return;
        if(stamp_retired(e, k_ring_magic))
            unlink(e);
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

    // A producer that returns to an entry its consumers kept takes the creator slot again.
    template<typename Entry>
    static acquire_bound retake_creator(Entry &e)
    {
        if(e.creator)
            return acquire_bound::none;
        const acquire_bound held = take_creator(e);
        if(held == acquire_bound::none)
            repair(e.ring);
        return held;
    }

    // Null for a mapping too small to hold a ring header.
    static control_header_t *header_of(std::span<std::byte> control) noexcept
    {
        if(control.size() < sizeof(control_header_t))
            return nullptr;
        return std::launder(reinterpret_cast<control_header_t *>(control.data()));
    }

    // A mapping too small for a header reads as a magic no ring carries.
    static std::uint64_t magic_of(std::span<std::byte> control) noexcept
    {
        const control_header_t *header = header_of(control);
        return header == nullptr ? ~std::uint64_t{0} : header->magic.load(std::memory_order_acquire);
    }

    template<typename Entry>
    static bool stamp_retired(Entry &e, std::uint64_t found) noexcept
    {
        control_header_t *header = header_of(e.control.bytes());
        return header != nullptr && header->magic.compare_exchange_strong(found, k_ring_retired, std::memory_order_release, std::memory_order_relaxed);
    }

    // The slab's name goes first: once the control name is gone a newcomer may create both afresh, and this ring must not
    // remove the newcomer's slab.
    template<typename Entry>
    static void unlink(Entry &e)
    {
        e.slab.unlink_name();
        e.control.unlink_name();
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

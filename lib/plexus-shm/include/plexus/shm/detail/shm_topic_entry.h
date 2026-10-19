#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ENTRY_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ENTRY_H

#include "plexus/shm/shm_channel.h"
#include "plexus/shm/waiter_loss.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <functional>
#include <unordered_map>

namespace plexus::shm::detail {

struct shm_topic_key
{
    std::string fqn;
    ring_direction direction;
    node_id publisher;

    bool operator==(const shm_topic_key &o) const noexcept
    {
        return direction == o.direction && publisher == o.publisher && fqn == o.fqn;
    }
};

struct shm_topic_key_hash
{
    std::size_t operator()(const shm_topic_key &k) const noexcept
    {
        std::size_t h = std::hash<std::string>{}(k.fqn) ^ static_cast<std::size_t>(k.direction) * 0x9e3779b9u;
        for(const std::byte b : k.publisher)
            h = h * 31u + static_cast<std::size_t>(b);
        return h;
    }
};

template<typename Entry>
using shm_topic_map = std::unordered_map<shm_topic_key, std::unique_ptr<Entry>, shm_topic_key_hash>;

// One live ring's owned state, pinned by the registry's unique_ptr so the non-movable
// ring/channel/notifier hold stable addresses. The channel must destruct BEFORE the notifier
// so a disarm-then-teardown keeps the wake off a destroyed subscriber. The waiter loss is shared
// with a report in flight, which may outlive the entry.
template<typename Broker, typename Notifier>
struct shm_topic_entry
{
    using notifier_type = Notifier;
    using deliver_fn    = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    shm_topic_entry()
            : control()
            , slab()
            , ring()
            , notify()
            , channel()
            , verdict(acquire_result::failed)
            , producers(0)
            , consumers(0)
            , creator(false)
            , sink()
            , loss(std::make_shared<waiter_loss>())
    {
    }

    typename Broker::region_handle control;
    typename Broker::region_handle slab;
    broadcast_ring ring;
    std::optional<Notifier> notify;
    std::optional<shm_channel<Notifier>> channel;
    acquire_result verdict;
    std::uint32_t producers;
    std::uint32_t consumers;
    bool creator;
    // Unset, the wake drain discards what it takes.
    deliver_fn sink;
    std::shared_ptr<waiter_loss> loss;

    void hold(ring_role role) noexcept
    {
        if(role != ring_role::consumer)
            ++producers;
        if(role != ring_role::producer)
            ++consumers;
    }

    void drop(ring_role role) noexcept
    {
        if(role != ring_role::consumer && producers > 0)
            --producers;
        if(role != ring_role::producer && consumers > 0)
            --consumers;
    }

    // The same disarm-first ordering as teardown, for the subscriber alone while the producer keeps sending.
    void disengage_consumer() noexcept
    {
        if(notify)
            notify->disarm();
        drop_cursor();
    }

    // Non-negotiable ordering: disarm the notifier BEFORE the channel (and the subscriber its drain touches) is
    // destroyed, so a wake racing teardown can never post a drain onto a destroyed subscriber.
    void teardown() noexcept
    {
        if(notify)
            notify->disarm();
        drop_cursor();
        channel.reset();
    }

    // The cursor goes inactive before its liveness slot is released, so a producer never sees it active and unheld
    // while this holder lives.
    void drop_cursor() noexcept
    {
        if(!channel)
            return;
        const std::optional<std::uint32_t> cursor = channel->waiter_slot();
        channel->release_subscriber();
        if(cursor)
            control.release_slot(cursor_slot(*cursor));
    }

    // The ONE place the cursor is taken, so the armed wake and the attach-time catch-up
    // share it without racing a second drainer.
    void deliver_pending()
    {
        if(!channel)
            return;
        if(sink)
            channel->drain(sink);
        else
        {
            deliver_fn discard = [](::plexus::wire_bytes<shm_slot_owner>) {};
            channel->drain(discard);
        }
    }
};

}

#endif

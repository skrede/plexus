#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ENTRY_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ENTRY_H

#include "plexus/shm/shm_channel.h"
#include "plexus/shm/region_naming.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/shm_slot_owner.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/wire_bytes.h"

#include "plexus/detail/compat.h"

#include <string>
#include <cstddef>
#include <optional>
#include <functional>

namespace plexus::shm::detail {

struct shm_topic_key
{
    std::string fqn;
    ring_direction direction;

    bool operator==(const shm_topic_key &o) const noexcept
    {
        return direction == o.direction && fqn == o.fqn;
    }
};

struct shm_topic_key_hash
{
    std::size_t operator()(const shm_topic_key &k) const noexcept
    {
        return std::hash<std::string>{}(k.fqn) ^ static_cast<std::size_t>(k.direction) * 0x9e3779b9u;
    }
};

// One live ring's owned state, pinned by the registry's unique_ptr so the non-movable
// ring/channel/notifier hold stable addresses. The channel must destruct BEFORE the notifier
// so a disarm-then-teardown keeps the wake off a destroyed subscriber.
template<typename Broker, typename Notifier>
struct shm_topic_entry
{
    using deliver_fn = plexus::detail::move_only_function<void(::plexus::wire_bytes<shm_slot_owner>)>;

    shm_topic_entry()
            : control()
            , slab()
            , ring()
            , notify()
            , channel()
            , verdict(acquire_result::failed)
            , refcount(0)
            , creator(false)
            , sink()
    {
    }

    typename Broker::region_handle control;
    typename Broker::region_handle slab;
    broadcast_ring ring;
    std::optional<Notifier> notify;
    std::optional<shm_channel<Notifier>> channel;
    acquire_result verdict;
    int refcount;
    bool creator;
    // Unset = the wake-drain discards (the send-only default).
    deliver_fn sink;

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

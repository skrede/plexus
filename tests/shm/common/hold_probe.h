#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_PROBE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_PROBE_H

#include "support/region_names.h"

#include "plexus/native/region_handle.h"
#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/io/observer.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/discovery/universe.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <atomic>
#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>

namespace plexus::testing {

// A read-only view of the publisher's ring, so a case can see how far the producer got and where each reader is.
struct ring_probe
{
    bool attach(const std::string &region_ns, const std::string &fqn, const plexus::node_id &publisher)
    {
        const std::string name = region_name(fqn, publisher, plexus::discovery::k_default_universe, region_ns);
        if(broker.attach(name, control) != plexus::shm::region_status::ok || broker.attach(name + ".s", slab) != plexus::shm::region_status::ok)
            return false;
        return plexus::shm::broadcast_ring::attach(control.bytes(), slab.bytes(), publisher, ring) == plexus::shm::loan_status::ok;
    }

    std::uint64_t tail() const
    {
        return ring.tail_position();
    }

    // The cursor a reader registered after the cursors in known, or known.size() when none did.
    std::uint32_t cursor_besides(const std::vector<std::uint32_t> &known)
    {
        for(std::uint32_t i = 0; i < ring.registered_high_water(); ++i)
            if(ring.header().cursors[i].active.load() != 0 && std::ranges::find(known, i) == known.end())
                return i;
        return static_cast<std::uint32_t>(known.size());
    }

    // Ends a run that is already lost: the producer stops waiting on this cursor.
    void release(std::uint32_t cursor)
    {
        ring.unregister_cursor(cursor);
    }

    std::uint64_t position(std::uint32_t cursor)
    {
        return ring.header().cursors[cursor].position.load(std::memory_order_acquire);
    }

    plexus::native::posix_shm_region_broker broker;
    plexus::native::region_handle control;
    plexus::native::region_handle slab;
    plexus::shm::broadcast_ring ring;
};

// Every message the publisher's egress shed for the topic; the counts reach it posted on the publisher's executor.
class topic_drops final : public plexus::io::observer
{
public:
    explicit topic_drops(const std::string &fqn)
            : m_hash(plexus::wire::fqn_topic_hash(fqn))
            , m_count(0)
    {
    }

    void on_drop(const plexus::io::detail::drop_event &event) override
    {
        if(event.topic_hash == m_hash)
            m_count.fetch_add(1, std::memory_order_relaxed);
    }

    std::uint32_t count() const noexcept
    {
        return m_count.load(std::memory_order_relaxed);
    }

private:
    std::uint64_t m_hash;
    std::atomic<std::uint32_t> m_count;
};

}

#endif

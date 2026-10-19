#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ACQUIRE_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_TOPIC_ACQUIRE_H

#include "plexus/shm/detail/shm_topic_open.h"
#include "plexus/shm/detail/shm_topic_entry.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/shm_acquire_result.h"

#include "plexus/io/same_host_geometry.h"

#include <memory>
#include <cstdint>
#include <utility>

namespace plexus::shm::detail {

// A key whose waiter was lost fails every acquire until its last holder releases it, so the next demand arms afresh. The
// first consumer of an entry a producer holds engages the entry's cursor there.
template<typename Entry, typename Report>
shm::acquire_result topic_reacquire(Entry &e, shm::ring_role role, Report &report, shm::acquire_failure &failure)
{
    if(const shm::arm_status lost = e.loss->status(); lost != shm::arm_status::armed)
    {
        failure = shm::acquire_failure{shm::acquire_bound::notifier, 0, 0, shm::region_status::ok, lost};
        return shm::acquire_result::failed;
    }
    if(role != shm::ring_role::producer && e.consumers == 0)
    {
        if(const shm::acquire_failure bound = topic_engage_consumer(e, report); bound.bound != shm::acquire_bound::none)
        {
            e.disengage_consumer();
            failure = bound;
            return shm::acquire_result::failed;
        }
    }
    e.hold(role);
    return e.verdict;
}

template<typename Registry>
shm::acquire_result topic_acquire(Registry &r, const shm_topic_key &k, shm::ring_role role, std::uint32_t max_payload, io::ring_geometry_mode mode, std::uint32_t consumer_capacity,
                                  shm::acquire_mode amode)
{
    r.m_last_failure = shm::acquire_failure{};
    if(auto it = r.m_entries.find(k); it != r.m_entries.end())
        return topic_reacquire(*it->second, role, r.m_fallback_cb, r.m_last_failure);

    auto e                            = std::make_unique<typename Registry::entry>();
    const shm::acquire_result verdict = topic_open_ring(r, *e, k, role, max_payload, mode, consumer_capacity, amode);
    if(verdict == shm::acquire_result::failed)
        return shm::acquire_result::failed;
    if(const shm::acquire_failure bound = topic_bind(r, *e, k, role, verdict); bound.bound != shm::acquire_bound::none)
    {
        e->teardown();
        r.m_last_failure = bound;
        return shm::acquire_result::failed;
    }
    r.m_entries.emplace(k, std::move(e));
    return verdict;
}

// The last consumer's release disarms and drops the cursor while any producer keeps sending; the entry tears down only
// once neither role holds it.
template<typename Registry>
void topic_release(Registry &r, const shm_topic_key &k, shm::ring_role role)
{
    auto it = r.m_entries.find(k);
    if(it == r.m_entries.end())
        return;
    auto &e                  = *it->second;
    const bool had_consumers = e.consumers > 0;
    e.drop(role);
    if(e.producers == 0 && e.consumers == 0)
    {
        e.teardown();
        r.m_entries.erase(it);
    }
    else if(had_consumers && e.consumers == 0)
        e.disengage_consumer();
}

}

#endif

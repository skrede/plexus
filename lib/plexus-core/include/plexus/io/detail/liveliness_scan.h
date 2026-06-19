#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_LIVELINESS_SCAN_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_LIVELINESS_SCAN_H

#include "plexus/io/liveness_event.h"
#include "plexus/io/detail/endpoint_liveness.h"

#include "plexus/node_id.h"

#include <map>
#include <cstdint>
#include <utility>
#include <optional>

namespace plexus::io::detail {

using deadline_key = std::pair<node_id, std::uint64_t>;

// Edge-latched deadline check: fire once when the data gap first exceeds the period; clear the
// latch when the gap falls back under it. A 0 period leaves the axis inert.
inline std::optional<liveness_event> check_deadline(const deadline_key &key, endpoint_liveness &s,
                                                    std::uint64_t now)
{
    if(s.deadline_period_ns == 0)
        return std::nullopt;
    const bool lapsed = now - s.last_data_seen_ns > s.deadline_period_ns;
    if(lapsed && !s.deadline_violated)
    {
        s.deadline_violated = true;
        return liveness_event{liveness_kind::missed_deadline, key.first, key.second,
                              s.deadline_period_ns};
    }
    if(!lapsed)
        s.deadline_violated = false;
    return std::nullopt;
}

// Edge-latched lease check: fire once when the presence gap first exceeds the lease; clear the
// latch when presence resumes. A 0 lease leaves the axis inert.
inline std::optional<liveness_event> check_lease(const node_id &id, endpoint_liveness &s,
                                                 std::uint64_t now)
{
    if(s.lease_ns == 0)
        return std::nullopt;
    const bool lapsed = now - s.last_seen_ns > s.lease_ns;
    if(lapsed && !s.lease_expired)
    {
        s.lease_expired = true;
        return liveness_event{liveness_kind::lease_expired, id, 0, s.lease_ns};
    }
    if(!lapsed)
        s.lease_expired = false;
    return std::nullopt;
}

// The per-tick scan over both axes: each axis fires exactly once per lapse via fire(event), keyed
// per-(endpoint, topic) for deadlines and per-endpoint for leases.
template<typename Fire>
void scan_liveness(std::map<deadline_key, endpoint_liveness> &deadlines,
                   std::map<node_id, endpoint_liveness> &leases, std::uint64_t now, Fire &&fire)
{
    for(auto &[key, state] : deadlines)
        if(auto ev = check_deadline(key, state, now))
            fire(*ev);
    for(auto &[id, state] : leases)
        if(auto ev = check_lease(id, state, now))
            fire(*ev);
}

}

#endif

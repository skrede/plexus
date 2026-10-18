#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_COORDINATOR_LANES_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_COORDINATOR_LANES_H

#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"

#include <cstdint>
#include <string_view>

namespace plexus::testing {

struct co_host_registry
{
    bool same_host_for(std::string_view) const noexcept
    {
        return true;
    }
};

// One live lane; the count falls when the coordinator drops it.
class counted_lane
{
public:
    explicit counted_lane(std::int32_t &live)
            : m_live(live)
    {
        ++m_live;
    }

    ~counted_lane()
    {
        --m_live;
    }

    counted_lane(const counted_lane &)            = delete;
    counted_lane &operator=(const counted_lane &) = delete;

private:
    std::int32_t &m_live;
};

// Drives a lane as far as the co-host peer would: a subscriber's demand meets its publisher's offer, a publisher's lane
// meets its subscriber's ready answer.
template<typename Coordinator>
void engage_demand(Coordinator &c, std::string_view node_name, std::string_view fqn, io::demand_role role)
{
    c.on_edge(node_name, fqn, io::demand_transition::up, role);
    c.on_companion(node_name, fqn, role == io::demand_role::subscriber ? io::companion_event::offered : io::companion_event::ready);
}

}

#endif

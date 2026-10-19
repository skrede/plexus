#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MEMBER_LANES_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MEMBER_LANES_H

#include "support/delaying_broker.h"
#include "support/coordinator_lanes.h"

#include "plexus/shm/shm_mux_member.h"
#include "plexus/shm/region_broker_concept.h"

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/node_upgrade_wiring.h"

#include <string>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus::testing {

// One member's send gate and its registry's reports bound to a coordinator the way the node binds them; the publisher's
// up-edge toward one co-host peer and that peer's ready answer run at construction.
template<typename Broker, typename Notifier>
struct member_lanes
{
    using member_type      = shm::shm_mux_member<Broker, Notifier>;
    using channel_type     = typename member_type::channel_type;
    using coordinator_type = io::upgrade_coordinator<co_host_registry, channel_type>;

    member_lanes(Broker &broker, std::string fqn, typename member_type::notifier_binder bind = member_type::registry_type::default_notifier_binder())
            : member(broker, io::reliability::reliable, io::congestion::block, std::move(bind))
            , reg()
            , lanes(reg)
            , topic(std::move(fqn))
    {
        lanes.on_gate([this](std::string_view fqn) { return plexus::detail::mint_send_lane(*this, member, fqn); });
        lanes.set_topic_hint(topic, io::dispatch_hint::frequent);
        plexus::detail::count_member_fallbacks(*this, member);
        lanes.on_edge("peer", topic, io::demand_transition::up, io::demand_role::publisher);
        lanes.on_companion("peer", topic, io::companion_event::ready);
    }

    coordinator_type &coordinator() noexcept
    {
        return lanes;
    }

    bool engaged()
    {
        return lanes.companion_for("peer", topic, 1).fits;
    }

    member_type member;
    co_host_registry reg;
    coordinator_type lanes;
    std::string topic;
};

// A host whose shared memory is full: every create is refused for want of backing memory.
struct full_broker
{
    using region_handle = delaying_handle;

    shm::region_status create(std::string_view, std::size_t, const shm::create_options &, region_handle &)
    {
        return shm::region_status::no_space;
    }

    shm::region_status attach(std::string_view, region_handle &)
    {
        return shm::region_status::not_found;
    }

    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>)
    {
    }
};

}

#endif

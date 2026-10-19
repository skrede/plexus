#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_COORDINATOR_LANES_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_COORDINATOR_LANES_H

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/upgrade_coordinator.h"

#include <chrono>
#include <memory>
#include <cstddef>
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

// A coordinator over counted lanes whose send gate declines its first declines mints as transient and whose policy
// follows allow; the release hook records how many lanes were still alive when it ran.
struct lane_end
{
    explicit lane_end(std::uint32_t declines = 0)
            : live(0)
            , mints(0)
            , released(0)
            , live_at_release(-1)
            , receives(0)
            , declines(declines)
            , allow(true)
            , c(registry)
    {
        c.on_gate([this](std::string_view) { return mint(); });
        c.on_receive_gate([this](std::string_view, std::string_view) { return io::upgrade_receive{[lane = std::make_unique<counted_lane>(receives)](std::uint64_t) { (void)lane; }}; });
        c.on_lane_released(
                [this](counted_lane &)
                {
                    ++released;
                    live_at_release = live;
                });
        c.on_policy([this](bool same_host, io::dispatch_hint) { return same_host && allow; });
        c.set_retry_scheduler([](std::chrono::milliseconds) {});
        c.set_topic_hint("alpha", io::dispatch_hint::frequent);
    }

    io::upgrade_mint<counted_lane> mint()
    {
        ++mints;
        if(declines > 0)
        {
            --declines;
            return io::upgrade_mint<counted_lane>{io::fallback_reason::transient};
        }
        return io::upgrade_mint<counted_lane>{std::make_unique<counted_lane>(live), [](std::size_t) { return true; }};
    }

    void offer(std::string_view peer)
    {
        c.on_edge(peer, "alpha", io::demand_transition::up, io::demand_role::publisher);
    }

    std::int32_t live;
    std::int32_t mints;
    std::int32_t released;
    std::int32_t live_at_release;
    std::int32_t receives;
    std::uint32_t declines;
    bool allow;
    co_host_registry registry;
    io::upgrade_coordinator<co_host_registry, counted_lane> c;
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

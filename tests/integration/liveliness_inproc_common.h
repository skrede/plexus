#ifndef HPP_GUARD_TESTS_INTEGRATION_LIVELINESS_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_LIVELINESS_INPROC_COMMON_H

// The shared inproc scaffold for the fused peer-liveliness verdict oracles on the deterministic
// virtual clock: the manual_clock/manual_policy pair, the engine alias, the id/config helpers, and a
// recording observer that opts into the liveliness edge and tallies the verdicts it receives. The
// verdict and arbitration scenario files build their two-engine nets over this scaffold and advance
// the clock by hand — never a wall-clock sleep.

#include "plexus/io/endpoint_id.h"
#include "plexus/io/observer.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include <vector>
#include <memory>
#include <chrono>
#include <cstdint>

namespace liveliness_inproc {

using plexus::io::combine;
using plexus::io::endpoint;
using plexus::io::endpoint_id;
using plexus::io::liveliness_verdict;
using plexus::io::liveliness_options;
using plexus::io::peer_liveliness_event;
using plexus::io::k_tick_granularity;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

struct manual_clock
{
    using duration                                   = std::chrono::nanoseconds;
    using rep                                        = duration::rep;
    using period                                     = duration::period;
    using time_point                                 = std::chrono::time_point<manual_clock>;
    [[maybe_unused]] static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept
    {
        return current;
    }
    static void reset() noexcept
    {
        current = time_point{};
    }
    static void advance(duration d) noexcept
    {
        current += d;
    }
};

struct manual_policy
{
    using executor_type     = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type        = inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine      = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto k_long_timeout  = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0x11FEu;

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// Inbound ids are minted in arrival order from 1; the n-th accept keys id[15] = n.
inline plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
}

inline std::uint64_t clock_now_ns()
{
    return static_cast<std::uint64_t>(manual_clock::now().time_since_epoch().count());
}

// A short awareness TTL pinned above the tick granularity so an advance past it crosses a sweep.
constexpr auto k_ttl = k_tick_granularity * 10;

inline liveliness_options opts(combine policy)
{
    liveliness_options live{};
    live.policy        = policy;
    live.awareness_ttl = std::chrono::nanoseconds(k_ttl);
    return live;
}

// Opts into the fused verdict edge and records every verdict it receives, so a scenario can count the
// alive/lost transitions and read the contributing-signal mask of the last one.
struct recording_liveliness_observer final : plexus::io::observer
{
    std::vector<peer_liveliness_event> events;
    bool observes = true;

    void on_peer_liveliness(const peer_liveliness_event &ev) override
    {
        events.push_back(ev);
    }

    bool observes_liveliness() const override
    {
        return observes;
    }

    int count(liveliness_verdict verdict) const
    {
        int n = 0;
        for(const auto &ev : events)
            if(ev.verdict == verdict)
                ++n;
        return n;
    }
};

}

#endif

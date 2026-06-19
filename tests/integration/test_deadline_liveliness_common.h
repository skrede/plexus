#ifndef HPP_GUARD_TESTS_INTEGRATION_DEADLINE_LIVELINESS_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_DEADLINE_LIVELINESS_COMMON_H

// The end-to-end deadline/liveliness behavioral gate on the deterministic virtual
// clock: two engines (a subscriber A, a publisher B) over one inproc bus. The
// subscriber drives subscribe(id, fqn, qos) with its OWN requested periods; the
// engine arms the one router-level monitor at the ready edge from the remembered
// demand; the receive path stamps presence on data and on the keepalive heartbeat;
// and the monitor edge-latched-fires missed-deadline / lease-expiry up the engine's
// on_liveness seam. Every timing leg is looped for reproducibility (a timing feature
// is never declared from a single run), every period is pinned >= k_tick_granularity
// so an advance crosses a tick expiry, and the clock is advanced by hand — never a
// wall-clock sleep.

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/liveness_event.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::subscriber_qos;
using plexus::io::liveness_event;
using plexus::io::liveness_kind;
using plexus::io::k_tick_granularity;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace deadline_fixture {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept { return current; }
    static void              reset() noexcept { current = time_point{}; }
    static void              advance(duration d) noexcept { current += d; }
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

// Periods pinned above the tick granularity so an advance crosses a tick expiry (M1).
constexpr auto k_period = k_tick_granularity * 3; // 300ms deadline
constexpr auto k_lease  = k_tick_granularity * 8; // 800ms lease
static_assert(k_period >= k_tick_granularity,
              "a deadline period below the tick granularity never crosses a tick");
static_assert(k_lease >= k_tick_granularity,
              "a lease below the tick granularity never crosses a tick");

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;
constexpr int           k_loops        = 50;

inline std::uint64_t ns_of(std::chrono::nanoseconds d)
{
    return static_cast<std::uint64_t>(d.count());
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

// A subscriber A and a publisher B over one bus. A demands B's topic with its chosen
// periods; B fans the topic back to A. The monitor lives on A (the receiving engine).
struct net
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};

    engine a;
    engine b;

    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    std::vector<liveness_event> events;

    net()
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed,
                /*eager=*/false)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed,
                /*eager=*/false)
    {
        a.listen(ep_a);
        b.listen(ep_b);
        a.note_peer(id_b, ep_b);
        a.on_liveness([this](const liveness_event &ev) { events.push_back(ev); });
    }

    void drive() { ex.drain(); }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        drive();
    }

    int count(liveness_kind kind) const
    {
        int n = 0;
        for(const auto &ev : events)
            if(ev.kind == kind)
                ++n;
        return n;
    }
};

}

#endif

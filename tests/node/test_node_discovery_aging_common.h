// The shared substrate for the discovery-aging proof, all on the manual virtual clock (no real
// multicast, no wall-clock sleep): the deterministic (announce_period, TTL) grid cell runner the
// recorded empirical sweep settles the locked default against, and the two-engine wired pair whose
// announcer goes silent and ages out of the observer's known().
#pragma once

#include "plexus/io/endpoint_id.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/testing/test_clock.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

namespace node_discovery_aging_fixture {

using plexus::io::default_discovery_ttl_ns;
using plexus::io::endpoint;
using plexus::io::endpoint_id;
using plexus::io::handshake_fsm_config;
using plexus::io::k_tick_granularity;
using plexus::io::known_peers;
using plexus::io::reconnect_config;
using plexus::testing::test_clock;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

constexpr std::uint64_t k_sec     = 1'000'000'000ull;
constexpr std::uint64_t k_tick_ns = static_cast<std::uint64_t>(k_tick_granularity.count()) * 1'000'000ull;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline endpoint ep_of(const char *addr)
{
    return endpoint{.scheme = "inproc", .address = addr};
}

// One grid cell's deterministic outcome on a virtual nanosecond cursor: a peer is re-announced every
// announce_period for K periods, then goes silent. The cell records the two properties the sweep
// settles the locked default against.
struct cell_outcome
{
    bool stayed_known_while_announcing;
    bool expired_after_silence;
    std::uint64_t expiry_within_ns; // how long after the last announce the entry was forgotten
};

inline cell_outcome run_cell(std::uint64_t announce_ns, std::uint64_t ttl_ns, int announce_count)
{
    known_peers table;
    const auto id     = make_id(0x42);
    std::uint64_t now = 0;

    bool stayed = true;
    for(int i = 0; i < announce_count; ++i)
    {
        table.note_peer(id, ep_of("svc"), now);
        now += announce_ns;
        table.expire_older_than(now > ttl_ns ? now - ttl_ns : 0, [](const plexus::node_id &) {});
        stayed = stayed && table.contains(id);
    }

    const std::uint64_t last_announce = now - announce_ns;
    bool expired                      = false;
    std::uint64_t expiry_at           = 0;
    for(std::uint64_t step = 0; step <= ttl_ns * 3; step += k_tick_ns)
    {
        const std::uint64_t t = last_announce + step;
        table.expire_older_than(t > ttl_ns ? t - ttl_ns : 0, [](const plexus::node_id &) {});
        if(!table.contains(id))
        {
            expired   = true;
            expiry_at = step;
            break;
        }
    }
    return {stayed, expired, expiry_at};
}

struct clock_policy
{
    using executor_type     = inproc_executor<test_clock> &;
    using byte_channel_type = inproc_channel<test_clock>;
    using timer_type        = inproc_timer<test_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<clock_policy>);

using transport_t = inproc_transport<test_clock>;
using engine      = plexus::io::routing_engine<clock_policy, transport_t, test_clock>;

constexpr auto k_long_timeout  = std::chrono::hours(1);
constexpr auto k_ttl           = k_tick_granularity * 10; // 1s, pinned above the granularity
constexpr std::uint64_t k_seed = 0xA61Eu;

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
}

inline std::uint64_t ttl_ns()
{
    return static_cast<std::uint64_t>(std::chrono::nanoseconds(k_ttl).count());
}

inline std::uint64_t clock_now_ns()
{
    return static_cast<std::uint64_t>(test_clock::now().time_since_epoch().count());
}

struct wired_pair
{
    inproc_bus<test_clock> bus;
    inproc_executor<test_clock> ex{bus};
    transport_t a_transport{ex, bus};
    transport_t b_transport{ex, bus};
    plexus::log::null_logger sink;
    engine observer;
    engine announcer;

    wired_pair()
            : observer(a_transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, ttl_ns())
            , announcer(b_transport, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed + 1, sink, false, plexus::io::global_default_max_message_bytes, ttl_ns())
    {
    }

    void advance(std::chrono::nanoseconds d)
    {
        for(auto elapsed = std::chrono::nanoseconds::zero(); elapsed < d; elapsed += k_tick_granularity)
        {
            test_clock::advance(k_tick_granularity);
            ex.drain();
        }
    }
};

} // namespace node_discovery_aging_fixture

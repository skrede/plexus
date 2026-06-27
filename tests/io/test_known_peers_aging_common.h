// The shared substrate for the TTL-driven awareness-aging oracle: a two-engine inproc pair over the
// manual virtual clock (the plexus::testing test_clock), constructed with a short test TTL. The
// scenario files exercise the silent-peer-ages-out, the re-announce-defers-expiry, the
// awareness-only (an active session survives the sweep) and the heartbeat-resets-TTL cases.
// Every timing assertion advances the clock by hand and drains the inproc step-executor — never a
// wall-clock sleep.
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

namespace known_peers_aging_fixture {

using plexus::io::endpoint;
using plexus::io::endpoint_id;
using plexus::io::handshake_fsm_config;
using plexus::io::k_tick_granularity;
using plexus::io::reconnect_config;
using plexus::testing::test_clock;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

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

constexpr auto k_long_timeout = std::chrono::hours(1);
// A small TTL pinned above the tick granularity so an advance past it actually crosses a tick.
constexpr auto k_ttl           = k_tick_granularity * 10; // 1s
constexpr std::uint64_t k_seed = 0xA61Eu;

inline std::uint64_t ttl_ns()
{
    return static_cast<std::uint64_t>(std::chrono::nanoseconds(k_ttl).count());
}

inline std::uint64_t clock_now_ns()
{
    return static_cast<std::uint64_t>(test_clock::now().time_since_epoch().count());
}

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

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000), std::nullopt, std::nullopt};
}

// Two engines over one bus, each with the short test TTL. B listens; A may dial it.
struct aging_pair
{
    inproc_bus<test_clock> bus;
    inproc_executor<test_clock> ex{bus};
    transport_t a_transport{ex, bus};
    transport_t b_transport{ex, bus};
    plexus::log::null_logger sink;
    engine a;
    engine b;

    aging_pair()
            : a(a_transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, ttl_ns())
            , b(b_transport, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed + 1, sink, false, plexus::io::global_default_max_message_bytes, ttl_ns())
    {
        b.listen({"inproc", "svc"});
    }

    // One big jump + one drain fires a SINGLE tick (the timer re-arms past the advanced now), so a
    // heartbeat round-trip cannot refresh across a long span. Stepping tick-by-tick fires a tick per
    // step — the realistic ~100ms heartbeat cadence the aging is designed against.
    void advance(std::chrono::nanoseconds d)
    {
        for(auto elapsed = std::chrono::nanoseconds::zero(); elapsed < d; elapsed += k_tick_granularity)
        {
            test_clock::advance(k_tick_granularity);
            ex.drain();
        }
    }
};

} // namespace known_peers_aging_fixture

// The liveliness_monitor unit oracle: the ONE router-level periodic ticker driven
// by the deterministic virtual clock (test_clock advance + inproc_executor drain),
// never a wall-clock sleep. It pins the single hardest correctness property — the
// edge-latched single-fire-per-lapse with re-arm-on-resume — plus the two-stamp
// distinctness, the inert 0-period, the no-fire-on-a-deregistered-endpoint lifetime
// invariant, and the zero-allocation steady-state stamp. Every timing assertion is
// looped for reproducibility, and every period is pinned >= k_tick_granularity so an
// advance actually crosses a tick expiry.
#pragma once

#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveness_event.h"

#include "plexus/testing/test_clock.h"
#include "plexus/testing/mock_policy.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

namespace liveliness_monitor_fixture {

using plexus::node_id;
using plexus::io::liveness_event;
using plexus::io::liveness_kind;
using plexus::io::k_tick_granularity;
using plexus::testing::test_clock;
using plexus::testing::mock_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;

using monitor = plexus::io::liveliness_monitor<mock_policy, test_clock>;

// A period pinned a tick above the granularity: an advance(P + a tick) reliably
// crosses a tick expiry so the scan fires. (M1: a sub-granularity period would never
// cross a tick boundary and the test would stick — never fire.)
constexpr auto k_period = k_tick_granularity * 3; // 300ms
constexpr auto k_lease  = k_tick_granularity * 5; // 500ms
static_assert(k_period >= k_tick_granularity, "a deadline period below the tick granularity never crosses a tick");
static_assert(k_lease >= k_tick_granularity, "a lease below the tick granularity never crosses a tick");

constexpr int k_loops = 50;

inline node_id make_id(std::uint8_t seed)
{
    node_id id{};
    id[0] = std::byte{seed};
    return id;
}

// A counting sink for fired events, separated by kind.
struct event_sink
{
    int missed_deadline = 0;
    int lease_expired   = 0;

    void attach(monitor &m)
    {
        m.on_liveness(
                [this](const liveness_event &ev)
                {
                    if(ev.kind == liveness_kind::missed_deadline)
                        ++missed_deadline;
                    else
                        ++lease_expired;
                });
    }
};

} // namespace liveliness_monitor_fixture

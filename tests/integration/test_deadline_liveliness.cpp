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

namespace {

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

std::uint64_t ns_of(std::chrono::nanoseconds d) { return static_cast<std::uint64_t>(d.count()); }

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config forever_cfg()
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

TEST_CASE("deadline liveliness: a data gap beyond the requested deadline fires exactly one "
          "missed-deadline")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // A subscribes to B's topic with its OWN requested deadline (the new overload).
        n.a.subscribe(n.id_b, "topic", subscriber_qos{.requested_deadline_ns = ns_of(k_period)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        // A's subscribe already drove B's on_subscribe -> attach_for_fanout, so B fans
        // "topic" back to A. Resolve B's accepted slot to drive the publishes.
        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);

        // A first data frame stamps the deadline clock.
        n.b.messages().publish("topic", as_bytes("d0"), b_to_a->session_id());
        n.drive();

        // A short gap (under the period) fires nothing.
        n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 0);

        // Resume, then lapse past the period: exactly one missed-deadline.
        n.b.messages().publish("topic", as_bytes("d1"), b_to_a->session_id());
        n.drive();
        n.advance(k_period + k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 1);

        // A further tick while still lapsed fires NO second event (edge-latched).
        n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 1);

        // Resume clears the latch; a second lapse re-fires.
        n.b.messages().publish("topic", as_bytes("d2"), b_to_a->session_id());
        n.drive();
        n.advance(k_period + k_tick_granularity);
        REQUIRE(n.count(liveness_kind::missed_deadline) == 2);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("deadline liveliness: a heartbeat lapse beyond the lease fires exactly one lease-expiry; "
          "a continuing heartbeat keeps it alive")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // A subscribes with only a lease requested (no deadline).
        n.a.subscribe(n.id_b, "topic", subscriber_qos{.requested_lease_ns = ns_of(k_lease)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        // With the tick emitting keepalive heartbeats across the live connection,
        // advancing several lease windows must NOT expire the lease (presence asserted).
        // Step a tick at a time so each tick's heartbeat refreshes presence before the
        // gap grows — the decoupled-token semantics (alive on the heartbeat alone, no
        // data flowing).
        const int steps = 2 * static_cast<int>(k_lease / k_tick_granularity);
        for(int s = 0; s < steps; ++s)
            n.advance(k_tick_granularity);
        REQUIRE(n.count(liveness_kind::lease_expired) == 0);

        // Now silence the peer: tear B's accepted slot down so no heartbeat AND no data
        // reaches A. (A's own tick keeps firing but stamps nothing for B once silent.)
        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);
        b_to_a->tear_down();
        n.drive();

        // A presence gap beyond the lease: exactly one lease-expiry. A's registration of
        // the B endpoint survives until A sees the disconnect; the silence is what fires.
        n.advance(k_lease + k_tick_granularity);
        const int expired = n.count(liveness_kind::lease_expired);
        REQUIRE(expired == 1);

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

TEST_CASE("deadline liveliness: a heartbeat refreshes the lease but not the deadline (the "
          "two-stamp end-to-end)")
{
    int proven = 0;
    for(int iter = 0; iter < k_loops; ++iter)
    {
        manual_clock::reset();
        net n;

        // Subscribe with BOTH a deadline P and a lease L (L > P): the keepalive
        // heartbeats keep presence alive while data lapses past the deadline.
        n.a.subscribe(n.id_b, "topic",
                      subscriber_qos{.requested_deadline_ns = ns_of(k_period),
                                     .requested_lease_ns    = ns_of(k_lease)});
        n.drive();
        REQUIRE(n.a.is_connected(n.id_b));

        const plexus::node_id b_inbound = []
        {
            auto id = make_id(0x00);
            id[15]  = std::byte{1};
            return id;
        }();
        auto *b_to_a = n.b.session_for(b_inbound);
        REQUIRE(b_to_a != nullptr);

        // One data frame stamps the deadline; then ONLY heartbeats flow (the tick emits
        // them, B publishes no more data).
        n.b.messages().publish("topic", as_bytes("d0"), b_to_a->session_id());
        n.drive();

        // Advance past the deadline (but under the lease): the data lapsed, but the
        // keepalive heartbeats kept presence.
        n.advance(k_period + k_tick_granularity);

        REQUIRE(n.count(liveness_kind::missed_deadline) == 1); // data lapsed
        REQUIRE(n.count(liveness_kind::lease_expired) == 0);   // heartbeat kept presence

        ++proven;
    }
    REQUIRE(proven == k_loops);
}

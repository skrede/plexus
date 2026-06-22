// The reconnect-survives-observer oracle on the manual virtual clock: an observer
// installed once on a node still receives lifecycle edges after a peer reconnect
// rebuild. The peer-session registry re-threads its observer wiring on every reconnect
// rebuild (the build context outlives each incarnation), so a single add_observer
// survives a forced down-then-up cycle. The reconnect is induced by a REAL transport
// drop — closing B's accepted end so A's dialer observes on_error — exactly as the
// drop-seam and reconnect oracles drive it; this oracle injects nothing.
//
// The post-reconnect edge under test (a fresh on_peer_reconnected / on_peer_ready) is
// delivered through the build-context-threaded observer route that already survives the
// rebuild today, so this case is a green regression guard the observer collapse must not
// break. The harness mirrors the peer-observer oracle.

#include "recording_observer.h"

#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::peer_kind;
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

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;
constexpr auto          k_ceiling      = std::chrono::milliseconds(10001);

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

// B keys an accepted session on a synthetic inbound identity minted in arrival order
// from 1; the n-th accept yields id[15] = n.
plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

struct two_node
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};

    plexus::log::null_logger sink;
    engine a{transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false};
    engine b{transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false};

    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    two_node()
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void drive() { ex.drain(); }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        drive();
    }
};

}

TEST_CASE("reconnect_observer inproc: an observer installed once survives a reconnect and receives "
          "the post-reconnect lifecycle edge",
          "[integration][reconnect][observer][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node           net;
        recording_observer rec;
        net.a.add_observer(rec);

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(rec.for_peer(net.id_b).connected == 1);
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // a zero-subscribe peer is ready on complete

        // A REAL transport drop: close B's accepted end so A observes on_error. Driving
        // the backoff tears down the dead session and re-handshakes through the SAME
        // build context the single add_observer wired.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();
        net.advance(k_ceiling);

        const auto &c = rec.for_peer(net.id_b);
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(c.connected == 1);   // still exactly one connect (not a second)
        REQUIRE(c.reconnected == 1); // the post-reconnect lifecycle edge reached the observer
        REQUIRE(c.ready == 2);       // ready re-armed across the reconnect
        REQUIRE(c.last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

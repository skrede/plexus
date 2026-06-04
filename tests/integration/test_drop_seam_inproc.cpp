// The deterministic inproc drop+surrender oracle on the manual virtual clock: it
// lifts the established-session transport-drop detection out of the harness and
// into the production receive path, and exercises the surrender->dead query the
// registry now exposes. Two (and, for the surrender leg, three) engines over one
// inproc bus, each owning its forwarders, its peer-session registry, its known-
// peers table and the dial-trigger hook. Every drop is induced by a REAL channel
// close on the REMOTE engine's accepted end (so the local dialer observes on_error
// from its own receive path) — this oracle injects nothing; it never calls the
// driver's established-drop trigger by hand. It proves, looped where behavioral:
//   - real-drop -> automatic re-dial: closing B's accepted end advances A's slot-B
//     attempt_count by exactly 1 with no injected drop, then re-handshakes a
//     strictly-later epoch on the next backoff;
//   - clean-close-no-redial: a clean tear_down on A's OWN B-session advances no
//     attempt_count (the m_torn_down guard + the silent-closing-end contract);
//   - surrender-without-collateral: enough real drops cross a max_attempts bound on
//     B's slot, marking is_dead(id_b) true while a co-resident live peer C stays
//     connected and is_dead(id_c) false.
// The scaffold (manual_clock/manual_policy, k_seed/k_long_timeout, the discovery
// stub, the two_node rendezvous + drive/advance idiom) mirrors the routing oracle.

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

// The virtual clock the handshake + backoff timers fire from, and the matching
// Policy — identical in shape to the routing oracle's manual_clock.
struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type = inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn) { ex.post(std::move(fn)); }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;   // fixed seed -> reproducible backoff
constexpr auto k_ceiling = std::chrono::milliseconds(10001);

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
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

reconnect_config bounded_cfg(std::uint32_t max_attempts)
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            max_attempts, std::nullopt};
}

// The inbound slot B keys an accepted session on: inbound ids are minted in arrival
// order from 1 (the registry's synthetic inbound identity). The n-th accept yields
// id[15] = n; the dialer's real node_id never reaches B's slot map here.
plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15] = std::byte{n};
    return id;
}

// The trivial discovery STUB: it feeds (node_id, endpoint) directly into note_peer,
// the awareness seam at its locked node_id->endpoint shape.
struct discovery_stub
{
    void announce(engine &to, const plexus::node_id &id, const endpoint &ep)
    {
        to.note_peer(id, ep);
    }
};

// A two-node rendezvous on one bus, the redial config selectable so the surrender
// leg can arm a bounded B slot. Member ORDER: bus/executor/transport BEFORE the
// engines so destruction unwinds the engines' channels before the bus.
struct two_node
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};

    engine a;
    engine b;

    discovery_stub discovery;
    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint ep_a{"inproc", "node-a"};
    endpoint ep_b{"inproc", "node-b"};

    explicit two_node(const reconnect_config &a_redial = forever_cfg())
        : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, a_redial, k_seed, false)
        , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, false)
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void drive() { ex.drain(); }
    void advance(std::chrono::nanoseconds d) { manual_clock::advance(d); drive(); }
};

}

TEST_CASE("inproc drop seam: a REAL partner-end close drives an automatic re-dial with no injection and a fresh epoch",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto pre_epoch = net.a.session_for(net.id_b)->session_id();
        const auto before = net.a.attempt_count(net.id_b);

        // Induce a REAL transport drop by closing B's ACCEPTED end: A's dialer
        // observes on_error from its own receive path and the production drop seam
        // routes that to A's slot-B driver — no hand-injected established-drop call.
        auto *b_inbound = net.b.session_for(inbound_slot(1));
        REQUIRE(b_inbound != nullptr);
        b_inbound->tear_down();
        net.drive();
        REQUIRE(net.a.attempt_count(net.id_b) == before + 1);

        // Drive the backoff: A re-dials and re-handshakes a strictly-later epoch.
        net.advance(k_ceiling);
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != pre_epoch);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc drop seam: a clean tear_down on the dialer's OWN session drives no re-dial",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto before = net.a.attempt_count(net.id_b);

        // A clean, self-initiated tear_down on A's OWN B-session sets m_torn_down
        // before closing the channel, so the drop seam swallows its own echo: no
        // re-dial amplification.
        net.a.session_for(net.id_b)->tear_down();
        net.drive();
        REQUIRE(net.a.attempt_count(net.id_b) == before);
        REQUIRE(!net.a.is_dead(net.id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc drop seam: crossing a surrender bound marks is_dead and re-dials nothing; a co-resident live peer is untouched",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    constexpr std::uint32_t k_max_attempts = 3;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        inproc_bus<manual_clock> bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t transport_a{ex, bus};
        transport_t transport_b{ex, bus};
        transport_t transport_c{ex, bus};

        // A's B slot is bounded (max_attempts) so enough real drops surrender it; C
        // is forever, so it is the live co-resident peer the surrender must not touch.
        engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, bounded_cfg(k_max_attempts), k_seed, false);
        engine b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, false);
        engine c(transport_c, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, false);

        plexus::node_id id_b = make_id(0xB2);
        plexus::node_id id_c = make_id(0xC3);
        endpoint ep_b{"inproc", "node-b"};
        endpoint ep_c{"inproc", "node-c"};
        a.listen({"inproc", "node-a"});
        b.listen(ep_b);
        c.listen(ep_c);

        a.note_peer(id_b, ep_b);
        a.note_peer(id_c, ep_c);
        a.reach(id_b);
        a.reach(id_c);
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        REQUIRE(a.is_connected(id_c));

        // Each real drop on B's current accepted end advances A's slot-B attempt
        // counter by 1; B mints the next inbound slot on each re-dial's re-accept.
        // Cross the bound: the last drop reports B dead and arms no further backoff.
        for(std::uint8_t n = 1; n <= k_max_attempts; ++n)
        {
            auto *b_inbound = b.session_for(inbound_slot(n));
            REQUIRE(b_inbound != nullptr);
            b_inbound->tear_down();
            ex.drain();
            if(n < k_max_attempts)
            {
                manual_clock::advance(k_ceiling);
                ex.drain();
            }
        }

        REQUIRE(a.is_dead(id_b));
        REQUIRE(a.attempt_count(id_b) == k_max_attempts);

        // No further re-dial after surrender: another advance moves nothing.
        const auto frozen = a.attempt_count(id_b);
        manual_clock::advance(k_ceiling);
        ex.drain();
        REQUIRE(a.attempt_count(id_b) == frozen);

        // Surrender without collateral: C is still connected and not dead.
        REQUIRE(a.is_connected(id_c));
        REQUIRE(!a.is_dead(id_c));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

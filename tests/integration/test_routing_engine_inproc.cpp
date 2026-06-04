// The deterministic two-node routing oracle on the manual virtual clock: two
// engines (node A, node B) over one inproc bus, each owning its forwarders, its
// peer-session registry, its known-peers table and the dial-trigger hook. A trivial
// discovery stub feeds (node_id, endpoint) straight into note_peer (the awareness
// seam this phase ships at its locked node_id->endpoint shape — no discovery-seam
// extension). The oracle proves, looped where behavioral:
//   - awareness-no-connect: note_peer records the entry and dials NOTHING;
//   - LAZY (default): no session until a demand call, then dial+handshake complete;
//   - EAGER (opt-in knob): note_peer ALONE dials+handshakes, no demand call;
//   - receive-path identity: a delivered message resolves to its own engine's sink;
//   - per-slot reconnect isolation: dropping ONE slot re-dials only that slot (its
//     attempt_count advances; another slot's does not — no set-wide loop);
//   - publish-does-not-dial: a publish to a known-but-unconnected peer opens no
//     connection; only subscribe/call/reach dial.
// Both knobs converge on the engine's single on_dialed -> build-from-record ->
// start() tail. This is the wave-0 routing oracle the engine headers satisfy.

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

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>

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
// Policy — identical in shape to the reconnect oracle's manual_clock.
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

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

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

// The trivial discovery STUB: it sidesteps the service_info-has-no-node_id gap by
// supplying (node_id, endpoint) DIRECTLY into an engine's note_peer. This is the
// awareness feed the locked node_id->endpoint table accepts — the production
// mdnspp-discovery -> node_id reconciliation is a later, separate concern.
struct discovery_stub
{
    void announce(engine &to, const plexus::node_id &id, const endpoint &ep)
    {
        to.note_peer(id, ep);
    }
};

// A two-node rendezvous on one bus: node A and node B each an engine over the
// shared executor/bus, each listening on its own endpoint. Member ORDER:
// bus/executor/transport BEFORE the engines so destruction unwinds the engines'
// channels before the bus they registered on. The eager flag selects the knob both
// engines run with.
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

    explicit two_node(bool eager = false)
        : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, eager)
        , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, eager)
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void drive() { ex.drain(); }
    void advance(std::chrono::nanoseconds d) { manual_clock::advance(d); drive(); }
};

}

TEST_CASE("inproc routing: note_peer records awareness and dials NOTHING (awareness without connect)",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;   // lazy (default): no eager dial off awareness

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        REQUIRE(net.a.known().contains(net.id_b));
        REQUIRE(net.a.known().lookup(net.id_b).has_value());
        REQUIRE(*net.a.known().lookup(net.id_b) == net.ep_b);
        REQUIRE(!net.a.has_session(net.id_b));   // awareness opened no connection
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: LAZY (default) opens no session until a demand call, then dials and completes",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;   // lazy default

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b));   // no demand yet -> no dial

        // Demand: reach the known-but-unconnected peer. NOW it dials, the inbound
        // bootstrap accepts, and the handshake completes on both ends.
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: a demand subscribe (not just reach) dials a known-but-unconnected peer and completes",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b));

        net.a.subscribe(net.id_b, "topic");
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: EAGER (opt-in knob) dials and completes off note_peer ALONE, with no demand call",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net(/*eager=*/true);

        // No reach/subscribe/call: awareness ALONE triggers the dial+handshake.
        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: a published message resolves to its own engine's receive sink (receive-path identity)",
          "[integration][routing][inproc]")
{
    manual_clock::reset();
    two_node net(/*eager=*/true);
    net.discovery.announce(net.a, net.id_b, net.ep_b);
    net.drive();
    REQUIRE(net.a.is_connected(net.id_b));

    auto *a_to_b = net.a.session_for(net.id_b);
    REQUIRE(a_to_b != nullptr);

    // B accepted one inbound slot; find it and wire its sink. The inbound session is
    // the one B holds with a complete handshake.
    plexus::node_id inbound = make_id(0x00);
    inbound[15] = std::byte{1};
    auto *b_inbound = net.b.session_for(inbound);
    REQUIRE(b_inbound != nullptr);
    REQUIRE(b_inbound->is_complete());

    std::vector<std::string> b_received;
    b_inbound->on_message([&](std::string_view, std::span<const std::byte> d) {
        b_received.emplace_back(to_string(d));
    });

    // B subscribes to A's topic (producer-side fanout), A publishes. The message
    // resolves through B's OWN node-shared forwarder to B's sink.
    REQUIRE(net.b.messages().attach_for_fanout(b_inbound->msg_peer(), "topic"));
    REQUIRE(net.a.messages().attach_for_fanout(a_to_b->msg_peer(), "topic"));
    net.drive();
    net.a.messages().publish("topic", as_bytes(std::string{"hello-b"}), a_to_b->session_id());
    net.drive();

    REQUIRE(b_received.size() == 1);
    REQUIRE(b_received.front() == "hello-b");
}

TEST_CASE("inproc routing: a publish to a known-but-unconnected peer's topic opens NO connection (publish does not dial)",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;   // lazy

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.drive();

        // Publish to a topic naming no connected subscriber: it must NOT dial.
        net.a.publish("topic", as_bytes(std::string{"speculative"}));
        net.drive();
        REQUIRE(!net.a.has_session(net.id_b));   // publish opened no connection
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc routing: a single slot's channel drop re-dials only that slot; another slot is untouched (per-slot isolation)",
          "[integration][routing][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        // A three-node-aware net: node A reaches both B and a second peer C (C is
        // node B's transport too — a second endpoint B listens on is unnecessary; we
        // assert single-slot isolation, so two independent slots on A suffice).
        inproc_bus<manual_clock> bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t transport_a{ex, bus};
        transport_t transport_b{ex, bus};
        transport_t transport_c{ex, bus};

        engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, false);
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

        const auto b_before = a.attempt_count(id_b);
        const auto c_before = a.attempt_count(id_c);
        const auto b_epoch = a.session_for(id_b)->session_id();

        // Drop ONLY slot B's channel: its driver re-dials. Slot C is untouched.
        a.registry().driver_for(id_b).on_channel_dropped();
        REQUIRE(a.attempt_count(id_b) == b_before + 1);   // B's slot advanced
        REQUIRE(a.attempt_count(id_c) == c_before);        // C's slot did NOT (no set-wide loop)

        // Drive the backoff: B re-dials and re-handshakes a FRESH epoch.
        manual_clock::advance(std::chrono::milliseconds(10001));
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        REQUIRE(a.session_for(id_b)->session_id() != b_epoch);
        REQUIRE(a.is_connected(id_c));                     // C never disturbed
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

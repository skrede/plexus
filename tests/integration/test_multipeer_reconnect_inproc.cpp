// The deterministic N>=3-peer concurrent-drop oracle on the
// manual virtual clock: one dialer engine A holding N established peer sessions (B,C,D,...), each
// peer its own engine over the shared inproc bus, all on a fixed seed. It widens the per-slot
// isolation proof to the SET: several established sessions drop AT ONCE (every remote
// accepted end is closed before the backoff fires — a single-FIFO bus makes that a
// matter of enqueueing all the closes, THEN draining once), and the registry must
// re-dial each dropped slot INDEPENDENTLY while leaving every survivor untouched. It
// proves, looped where behavioral:
//   - independence under concurrent drops: each dropped peer's per-node_id
//     attempt_count advances by exactly 1 and re-handshakes a strictly-later epoch on
//     its OWN backoff, while every survivor's attempt_count + session_id are unchanged
//     and it is never re-dialed (no set-wide reconnect loop);
//   - surrender without collateral: one peer crossing a small max_attempts bound is
//     is_dead while every co-resident live peer stays is_connected and is_dead==false;
//   - per-peer straggler: after a peer re-handshakes, a frame carrying its PREVIOUS
//     epoch is dropped by the staleness gate while a current-epoch frame is delivered.
// Every drop is induced by a REAL channel close on the REMOTE engine's accepted end
// (so A's dialer observes on_error from its own receive path) — this oracle injects
// nothing; it never calls the driver's established-drop trigger by hand. The scaffold
// (manual_clock/manual_policy, k_seed/k_long_timeout, the discovery stub, make_cfg/
// make_id/forever_cfg) mirrors the routing + drop-seam oracles.

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
// Policy — identical in shape to the routing + drop-seam oracles' manual_clock.
struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

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
constexpr std::uint64_t k_seed = 0xC0FFEEu; // fixed seed -> reproducible backoff
constexpr auto k_ceiling       = std::chrono::milliseconds(10001);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Synthesize a unidirectional "topic" data frame carrying a chosen session_id via
// the production framing path, so feeding it to a receiver's on_receive exercises
// the REAL staleness gate (mirrors the single-connection reconnect oracle).
std::vector<std::byte> make_data_frame(const std::string &payload, std::uint64_t session_id)
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex(bus);
    plexus::log::null_logger sink;
    plexus::io::message_forwarder<manual_policy> framer{sink};
    inproc_channel<manual_clock> capture(ex);
    inproc_channel<manual_clock> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    plexus::io::message_forwarder<manual_policy>::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

reconnect_config bounded_cfg(std::uint32_t max_attempts)
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), max_attempts, std::nullopt};
}

// A peer engine keys each accepted session on a synthetic inbound identity minted in
// arrival order from 1; a fresh peer engine's FIRST (and only) accept is id[15]=n.
plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
    return id;
}

// A trivial discovery stub feeding (node_id, endpoint) straight into note_peer.
struct discovery_stub
{
    void announce(engine &to, const plexus::node_id &id, const endpoint &ep)
    {
        to.note_peer(id, ep);
    }
};

// One peer engine on the shared bus: its own transport, a distinct node_id/endpoint,
// minted from a per-peer seed. Member ORDER: transport BEFORE the engine so the
// engine's channels unwind before the transport.
struct peer_node
{
    transport_t transport;
    plexus::log::null_logger sink;
    engine eng;
    plexus::node_id id;
    endpoint ep;

    peer_node(inproc_executor<manual_clock> &ex, inproc_bus<manual_clock> &bus, std::uint8_t seed)
            : transport(ex, bus)
            , eng(transport, ex, make_cfg(seed), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , id(make_id(seed))
            , ep{"inproc", "node-" + std::to_string(static_cast<unsigned>(seed))}
    {
        eng.listen(ep);
    }
};

// An N-peer net: a dialer engine A and N peer engines on one bus. A reaches every
// peer to a complete session. Member ORDER: bus/executor/transport_a BEFORE engine A,
// and the peers (each owning its own transport) declared after so destruction unwinds
// every engine's channels before the bus. A's redial config is selectable so the
// surrender leg can arm a bounded dialer.
struct multipeer_net
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    plexus::log::null_logger sink;
    engine a;
    discovery_stub discovery;
    std::vector<std::unique_ptr<peer_node>> peers;

    multipeer_net(std::size_t n, const reconnect_config &a_redial = forever_cfg())
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, a_redial, k_seed, sink, false)
    {
        a.listen({"inproc", "node-a"});
        for(std::size_t i = 0; i < n; ++i)
        {
            // Peer seeds start at 0xB0 so they are distinct from A (0xA1) and from
            // the synthetic inbound key space (0x00).
            auto seed = static_cast<std::uint8_t>(0xB0 + i);
            peers.push_back(std::make_unique<peer_node>(ex, bus, seed));
            discovery.announce(a, peers.back()->id, peers.back()->ep);
            a.reach(peers.back()->id);
        }
        ex.drain();
    }

    void drive()
    {
        ex.drain();
    }
    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        drive();
    }

    peer_node &peer(std::size_t i)
    {
        return *peers[i];
    }

    // The injection-free real drop verb: close the i-th peer engine's FIRST accepted
    // inbound session, so A's dialer observes the partner close from its own receive
    // path and the production on_transport_drop seam routes it to A's slot driver — the
    // registry wires that seam on every dialed slot, so the test injects nothing.
    void drop_peer(std::size_t i)
    {
        auto *inbound = peers[i]->eng.session_for(inbound_slot(1));
        REQUIRE(inbound != nullptr);
        inbound->tear_down();
    }
};

}

TEST_CASE("multipeer inproc: concurrent real drops re-dial each dropped slot independently; "
          "survivors are bit-for-bit undisturbed",
          "[integration][multipeer][inproc]")
{
    constexpr int k_iterations      = 100;
    constexpr std::size_t k_n       = 3; // N>=3 established peers
    constexpr std::size_t k_dropped = 2; // K>=2 dropped concurrently; 1 survives
    int proven                      = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        multipeer_net net(k_n);
        for(std::size_t i = 0; i < k_n; ++i)
            REQUIRE(net.a.is_connected(net.peer(i).id));

        std::array<std::uint32_t, k_n> before{};
        std::array<std::uint64_t, k_n> epoch{};
        for(std::size_t i = 0; i < k_n; ++i)
        {
            before[i] = net.a.attempt_count(net.peer(i).id);
            epoch[i]  = net.a.session_for(net.peer(i).id)->session_id();
        }

        // Concurrent drop: enqueue ALL K closes on the accepted ends, THEN drain once
        // (a single-FIFO bus would otherwise sequence them if drained between closes).
        for(std::size_t i = 0; i < k_dropped; ++i)
            net.drop_peer(i);
        net.drive();

        // Independence: each dropped peer's attempt_count advanced by exactly 1; every
        // survivor's is unchanged (no set-wide loop, no spurious re-dial).
        for(std::size_t i = 0; i < k_dropped; ++i)
            REQUIRE(net.a.attempt_count(net.peer(i).id) == before[i] + 1);
        for(std::size_t i = k_dropped; i < k_n; ++i)
            REQUIRE(net.a.attempt_count(net.peer(i).id) == before[i]);

        // Drive the backoff: each dropped peer re-dials and re-handshakes a FRESH epoch
        // on its own slot; every survivor stays connected with its epoch unchanged.
        net.advance(k_ceiling);
        for(std::size_t i = 0; i < k_dropped; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() != epoch[i]);
        }
        for(std::size_t i = k_dropped; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(net.a.session_for(net.peer(i).id)->session_id() == epoch[i]);
            REQUIRE(net.a.attempt_count(net.peer(i).id) == before[i]);
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("multipeer inproc: one peer crossing a surrender bound is is_dead while every live peer "
          "is untouched",
          "[integration][multipeer][inproc]")
{
    constexpr int k_iterations             = 100;
    constexpr std::size_t k_n              = 3;
    constexpr std::uint32_t k_max_attempts = 3;
    int proven                             = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        // A's slots are all bounded (one node-wide redial config); only peer 0 is
        // driven past the bound, so the others stay live as the co-resident set the
        // surrender must not perturb.
        multipeer_net net(k_n, bounded_cfg(k_max_attempts));
        for(std::size_t i = 0; i < k_n; ++i)
            REQUIRE(net.a.is_connected(net.peer(i).id));

        // Each real drop on peer 0's current accepted end advances A's slot-0 attempt
        // counter by 1; the peer mints the next inbound slot on each re-dial's accept.
        // Cross the bound: the last drop reports the slot dead and arms no backoff.
        for(std::uint8_t n = 1; n <= k_max_attempts; ++n)
        {
            auto *inbound = net.peer(0).eng.session_for(inbound_slot(n));
            REQUIRE(inbound != nullptr);
            inbound->tear_down();
            net.drive();
            if(n < k_max_attempts)
                net.advance(k_ceiling);
        }

        REQUIRE(net.a.is_dead(net.peer(0).id));
        REQUIRE(net.a.attempt_count(net.peer(0).id) == k_max_attempts);

        // No further re-dial after surrender: another advance moves nothing on slot 0.
        const auto frozen = net.a.attempt_count(net.peer(0).id);
        net.advance(k_ceiling);
        REQUIRE(net.a.attempt_count(net.peer(0).id) == frozen);

        // Surrender without collateral: every other peer is connected and not dead.
        for(std::size_t i = 1; i < k_n; ++i)
        {
            REQUIRE(net.a.is_connected(net.peer(i).id));
            REQUIRE(!net.a.is_dead(net.peer(i).id));
        }
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("multipeer inproc: a reconnected peer's previous-epoch straggler is dropped; the "
          "current-epoch frame is delivered",
          "[integration][multipeer][inproc]")
{
    constexpr int k_iterations = 100;
    constexpr std::size_t k_n  = 3;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        multipeer_net net(k_n);
        for(std::size_t i = 0; i < k_n; ++i)
            REQUIRE(net.a.is_connected(net.peer(i).id));

        // Reconnect peer 0 (a real drop -> backoff -> fresh epoch) while the others
        // stay up. The staleness gate on A's slot-0 latches the PEER's epoch, so the
        // dead epoch the straggler must carry is peer 0's INBOUND session_id before the
        // drop (A's own local epoch is a separate well).
        auto &p               = net.peer(0);
        auto *a_to_p          = net.a.session_for(p.id);
        const auto dead_local = a_to_p->session_id();

        std::vector<std::string> a_received;
        a_to_p->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(std::string{reinterpret_cast<const char *>(d.data()), d.size()}); });

        net.drop_peer(0);
        net.drive(); // process the real drop: slot 0's driver arms its backoff
        net.advance(k_ceiling);
        REQUIRE(net.a.is_connected(p.id));
        a_to_p = net.a.session_for(p.id);
        REQUIRE(a_to_p->session_id() != dead_local); // A's local epoch advanced

        // Re-wire the sink on the reconnected slot-0 session and latch the live PEER
        // epoch with a real publish from the peer's reconnected inbound session. The
        // gate compares an incoming frame's session_id against THIS latched peer epoch.
        a_to_p->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(std::string{reinterpret_cast<const char *>(d.data()), d.size()}); });
        auto *p_inbound = p.eng.session_for(inbound_slot(2)); // the re-accept is slot 2
        REQUIRE(p_inbound != nullptr);
        const auto live_epoch = p_inbound->session_id();
        REQUIRE(net.a.messages().attach_for_fanout(a_to_p->msg_peer(), "topic"));
        REQUIRE(p.eng.messages().attach_for_fanout(p_inbound->msg_peer(), "topic"));
        net.drive();
        p.eng.messages().publish("topic", as_bytes(std::string{"live"}), live_epoch);
        net.drive();
        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == "live");
        REQUIRE(a_to_p->peer_session_id() == live_epoch);

        // A straggler carrying a PREVIOUS (dead) incarnation's epoch — a value distinct
        // from the latched live one — is dropped by the per-peer staleness gate. It is
        // fed verbatim through A's slot-0 production receive path (on_receive runs the
        // gate before the router), with no second delivery.
        const std::uint8_t stale_epoch = (live_epoch == 1) ? std::uint8_t{2} : std::uint8_t(live_epoch - 1);
        auto straggler                 = make_data_frame("dead-incarnation", stale_epoch);
        a_to_p->on_receive(straggler);
        net.drive();
        REQUIRE(a_received.size() == 1); // the previous-epoch frame is DROPPED

        // A frame carrying the CURRENT epoch is delivered (the gate dropped only stale).
        auto current = make_data_frame("again", live_epoch);
        a_to_p->on_receive(current);
        net.drive();
        REQUIRE(a_received.size() == 2);
        REQUIRE(a_received.back() == "again");
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

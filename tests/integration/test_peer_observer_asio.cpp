// The live-TCP peer-observer behavior oracle over asio loopback. A recording observer
// (shared header) is registered on a real routing_engine driven over asio_transport
// with NO hand-dial, and the same lifecycle + readiness matrix the inproc suite forces
// is proven over a real socket — connected/reconnected/disconnected/rejected/dead plus
// the readiness loop (ready once per cycle over the REAL subscribe->ack loop with no
// faked attaches, double-fire prevention across a real reconnect, the premature-ready
// window, the zero-subscribe immediate ready, the accepted-peer edge subset, and
// posted re-entrant delivery). Every edge is POSTED, so each assertion is made AFTER a
// bounded pump reaches the predicate. The behavioral paths loop N>=100 in-body; the
// ctest invocation is re-run >=3 process runs (a live-networking claim is never made
// from one run). The forged-frame underflow case stays inproc-only (it forges a frame).
// The harness mirrors the real-TCP routing oracle.

#include "recording_observer.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_channel.h"

#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

using pio::endpoint;
using pio::handshake_fsm_config;
using pio::reconnect_config;
using pio::peer_kind;
using pio::handshake_outcome;
using engine = pio::routing_engine<pasio::asio_policy, pasio::asio_transport, std::chrono::steady_clock>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed, std::uint8_t version = 1, std::uint8_t compatible = 1)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = version, .version_minor = 0,
                                .compatible_version_major = compatible, .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15] = std::byte{n};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000),
                            std::nullopt, std::nullopt};
}

reconnect_config bounded_cfg(std::uint32_t max_attempts)
{
    return reconnect_config{std::chrono::milliseconds(20), std::chrono::milliseconds(80),
                            max_attempts, std::nullopt};
}

struct asio_node
{
    ::asio::io_context &io;
    pasio::asio_transport transport;
    engine eng;

    asio_node(::asio::io_context &shared, std::uint8_t id_seed, bool eager,
              const reconnect_config &redial = forever_cfg(), std::uint8_t compatible = 1)
        : io(shared)
        , transport(shared)
        , eng(transport, shared, make_cfg(id_seed, 1, compatible), k_long_timeout, redial, k_seed, eager)
    {
        eng.listen({"tcp", "127.0.0.1:0"});
    }

    endpoint listen_ep() const { return {"tcp", "127.0.0.1:" + std::to_string(transport.port())}; }
};

std::uint16_t reserve_closed_port()
{
    ::asio::io_context probe_io;
    pasio::asio_transport probe{probe_io};
    probe.listen({"tcp", "127.0.0.1:0"});
    const auto port = probe.port();
    probe.close();
    return port;
}

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred,
                std::chrono::seconds bound_secs = std::chrono::seconds(5))
{
    auto bound = std::chrono::steady_clock::now() + bound_secs;
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

void settle(::asio::io_context &io, std::chrono::milliseconds window = std::chrono::milliseconds(40))
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

TEST_CASE("observer over asio: connected fires once and ready fires immediately for a zero-subscribe peer over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(c.connected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.ready == 1);
        REQUIRE(c.last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: a real socket drop+redial fires reconnected (NOT a second connected) and a disconnected over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(rec.for_peer(id_b).connected == 1);

        // A REAL transport drop: close B's accepted socket so A observes the drop and
        // re-dials over a fresh socket, re-handshaking. reconnected fires (not a second
        // connected); the dead session's tear_down fires disconnected.
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io, [&] { return rec.for_peer(id_b).reconnected == 1 && a.eng.is_connected(id_b); });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(c.connected == 1);
        REQUIRE(c.reconnected == 1);
        REQUIRE(c.disconnected == 1);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: dead fires once when the driver surrenders against an unbindable endpoint over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        // A dials a reserved-then-closed port that never binds: every dial is refused,
        // so the bounded driver surrenders and fires dead.
        asio_node a{io, 0xA1, /*eager=*/false, bounded_cfg(/*max_attempts=*/3)};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        const auto dead_port = reserve_closed_port();
        a.eng.note_peer(id_b, {"tcp", "127.0.0.1:" + std::to_string(dead_port)});
        a.eng.reach(id_b);
        pump_until(io, [&] { return rec.for_peer(id_b).dead == 1; });

        REQUIRE(a.eng.is_dead(id_b));
        REQUIRE(rec.for_peer(id_b).dead == 1);
        REQUIRE(rec.for_peer(id_b).connected == 0);   // never connected
        REQUIRE(rec.for_peer(id_b).last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: rejected fires once carrying the real refusal reason on a version-incompatible handshake over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        // A requires compatible >= 2; B advertises version 1, so A rejects B's accept
        // response over the wire -> A fires rejected(reject_version). A's redial is
        // bounded so a rejected (protocol-error) close does not spin forever.
        asio_node a{io, 0xA1, /*eager=*/false, bounded_cfg(2), /*compatible=*/2};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return rec.for_peer(id_b).rejected >= 1; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(c.rejected >= 1);
        REQUIRE(c.last_reason == handshake_outcome::reject_version);
        REQUIRE(c.connected == 0);
        REQUIRE(!a.eng.is_connected(id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: on_peer_ready over the REAL loop, then the awaited publish lands over real TCP (first-publish-loss-free)",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload = "ready-then-publish-over-tcp";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        // Subscribe through the engine (the REAL counted loop, NO faked attach): the
        // lazy subscribe triggers the dial, is remembered until complete, then flushed
        // so ready fires after its ack.
        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(rec.for_peer(id_b).ready == 1);

        // The awaited publish: A is the subscriber; B publishes and the frame lands at
        // A's per-session sink — proving the subscribe round-trip wired the fan-out.
        auto *a_to_b = a.eng.session_for(id_b);
        std::vector<std::string> a_received;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(to_string(d)); });

        auto *b_inbound = b.eng.session_for(inbound_slot(1));
        REQUIRE(b_inbound != nullptr);
        settle(io);   // let B's producer-side fanout settle from A's subscribe
        b.eng.messages().publish("topic", as_bytes(payload), b_inbound->session_id());
        pump_until(io, [&] { return !a_received.empty(); });

        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: ready fires EXACTLY once per cycle across a real reconnect (count == 2 over two cycles)",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(rec.for_peer(id_b).ready == 1);   // cycle 1

        // Real socket drop + re-dial: the fresh incarnation re-arms and resurrects the
        // remembered subscribe through the counted path, firing ready a SECOND time.
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io, [&] { return rec.for_peer(id_b).ready == 2; });

        const auto &c = rec.for_peer(id_b);
        REQUIRE(c.ready == 2);
        REQUIRE(c.reconnected == 1);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: premature-ready window — ready stays 1 across the reconnect-complete predicate before the resurrected acks drain, becomes 2 only after over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        a.eng.add_observer(rec);

        // Connect first (zero-subscribe ready fires once), then subscribe N>1 topics on
        // the live session so each is remembered. The reconnect resurrects all N.
        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b) && rec.for_peer(id_b).ready == 1; });
        REQUIRE(rec.for_peer(id_b).ready == 1);   // cycle 1: zero-subscribe ready
        a.eng.subscribe(id_b, "topic-1");
        a.eng.subscribe(id_b, "topic-2");
        a.eng.subscribe(id_b, "topic-3");
        settle(io);
        REQUIRE(rec.for_peer(id_b).ready == 1);   // late subscribes do NOT re-fire

        // Real socket drop. The reconnect-complete predicate (reconnected fired) marks
        // the resurrection window: on_complete ran resubscribe_all (counter now N) and
        // held ready, but the resurrected acks have not round-tripped, so ready is
        // STILL 1. A regression that bypassed the counted path would prematurely fire
        // ready (making it 2) at this point.
        b.eng.session_for(inbound_slot(1))->tear_down();
        pump_until(io, [&] { return rec.for_peer(id_b).reconnected == 1; });
        REQUIRE(rec.for_peer(id_b).reconnected == 1);
        REQUIRE(rec.for_peer(id_b).ready == 1);   // held during the resurrection window

        // Drain the resurrected subscribe_responses: ready becomes 2 only now.
        pump_until(io, [&] { return rec.for_peer(id_b).ready == 2; });
        REQUIRE(rec.for_peer(id_b).ready == 2);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: an accepted (inbound) peer fires connected/disconnected/ready but NEVER reconnected or dead over real TCP",
          "[integration][observer][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        recording_observer rec;
        b.eng.add_observer(rec);   // observe the ACCEPTING node's inbound slot

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        const auto inbound = inbound_slot(1);
        pump_until(io, [&] { return rec.for_peer(inbound).connected == 1 && rec.for_peer(inbound).ready == 1; });
        {
            const auto &c = rec.for_peer(inbound);
            REQUIRE(c.connected == 1);
            REQUIRE(c.ready == 1);
            REQUIRE(c.last_kind == peer_kind::accepted);
        }

        // Drop from A's side: B's accepted session tears down (disconnected) but owns
        // no driver, so NEVER reconnect/dead.
        a.eng.session_for(id_b)->tear_down();
        pump_until(io, [&] { return rec.for_peer(inbound).disconnected == 1; });
        settle(io);   // give any (erroneous) reconnect/dead a chance to fire

        const auto &c = rec.for_peer(inbound);
        REQUIRE(c.disconnected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.dead == 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("observer over asio: calling engine.subscribe from inside an observer callback is posted-safe over real TCP (no re-entrancy crash)",
          "[integration][observer][asio]")
{
    struct reentrant_observer final : public plexus::io::peer_observer
    {
        engine *eng{nullptr};
        plexus::node_id target{};
        int connected{0};
        void on_peer_connected(const plexus::node_id &, std::string_view, peer_kind) override
        {
            ++connected;
            if(eng)
                eng->subscribe(target, "late-topic");
        }
    };

    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        reentrant_observer obs;
        obs.eng = &a.eng;
        obs.target = id_b;
        a.eng.add_observer(obs);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.reach(id_b);
        pump_until(io, [&] { return obs.connected == 1; });
        settle(io);   // the nested subscribe runs on a later turn — no crash

        REQUIRE(obs.connected == 1);
        REQUIRE(a.eng.is_connected(id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

// Gated real-TCP routing oracle over asio loopback. It drives the routing_engine
// over asio_transport with NO hand-dial — the engine itself opens the connection,
// never a manual connect on a raw socket, never a spin on a server-channel handle:
// the engine's reach -> driver.start -> transport.dial -> on_dialed ->
// registry build-from-record -> start() tail runs over real TCP. It proves BOTH
// dial knobs plexus<->plexus on the wall clock:
//   - LAZY (default): note_peer records awareness and opens NO connection; only a
//     demand call (reach/subscribe) dials, then the handshake completes and a real
//     published message carrying the minted epoch flows;
//   - EAGER (opt-in): note_peer ALONE dials+handshakes with no demand call.
// It ALSO proves the receive-path source-peer identity leg over the wire: one node
// connected to TWO distinct remote peers resolves each delivered frame to its OWN
// source-peer identity in the per-session sinks — peer-X's message lands attributed
// to X and peer-Y's to Y, no cross-attribution. That two-peer leg dials both peers
// near-simultaneously (overlapping in-flight dials whose TCP connects can complete
// out of order), so it is also the adversary for the dial-completion correlation:
// the engine must route each completed channel back to ITS slot by the dial
// endpoint, not by arrival order. The behavioral happy paths loop N>=100 in-body;
// the ctest invocation is re-run >=3 process runs (a live-networking claim is never
// made from one run).

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
    return reconnect_config{std::chrono::milliseconds(50), std::chrono::milliseconds(2000),
                            std::nullopt, std::nullopt};
}

// One asio engine on a shared io_context, listening on an ephemeral port. Member
// ORDER: io_context/transport BEFORE the engine so destruction unwinds the engine's
// channels/sessions before the io_context they borrow.
struct asio_node
{
    ::asio::io_context &io;
    pasio::asio_transport transport;
    engine eng;

    // listen_now=false defers the listen so the caller can bring the acceptor up
    // LATE (the refused-then-up path that forces an out-of-order dial completion).
    asio_node(::asio::io_context &shared, std::uint8_t id_seed, bool eager, bool listen_now = true)
        : io(shared)
        , transport(shared)
        , eng(transport, shared, make_cfg(id_seed), k_long_timeout, forever_cfg(), k_seed, eager)
    {
        if(listen_now)
            eng.listen({"tcp", "127.0.0.1:0"});
    }

    void listen_on(std::uint16_t port) { eng.listen({"tcp", "127.0.0.1:" + std::to_string(port)}); }
    endpoint listen_ep() const { return {"tcp", "127.0.0.1:" + std::to_string(transport.port())}; }
};

// Reserve a free loopback port, then close it so a dial there is refused until a
// listener rebinds it (reuse_address is set on the acceptor).
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
void pump_until(::asio::io_context &io, Pred pred)
{
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

void settle(::asio::io_context &io, std::chrono::milliseconds window = std::chrono::milliseconds(30))
{
    auto bound = std::chrono::steady_clock::now() + window;
    while(std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

TEST_CASE("routing over asio: LAZY opens no connection until a demand call, then dials and completes over real TCP",
          "[integration][routing][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);

        // Awareness alone: no demand, so the lazy engine dials NOTHING.
        a.eng.note_peer(id_b, b.listen_ep());
        settle(io);
        REQUIRE(!a.eng.has_session(id_b));

        // Demand: reach the known-but-unconnected peer. NOW it dials over real TCP,
        // the inbound bootstrap accepts, and the handshake completes both ends.
        a.eng.reach(id_b);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.session_for(id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("routing over asio: a demand subscribe carries a real published message with the minted epoch over real TCP",
          "[integration][routing][asio]")
{
    constexpr int k_iterations = 100;
    const std::string payload = "routed-published-bytes-over-tcp";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        const auto id_b = make_id(0xB2);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.subscribe(id_b, "topic");
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));

        // B (the inbound side) wires its sink and producer-side fanout; A's session
        // attached on subscribe. B publishes; the frame resolves through A's own
        // node-shared forwarder to A's per-session sink carrying B's minted epoch.
        auto *a_to_b = a.eng.session_for(id_b);
        REQUIRE(a_to_b != nullptr);
        std::vector<std::string> a_received;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) {
            a_received.emplace_back(to_string(d));
        });

        const auto inbound = [] { auto id = make_id(0x00); id[15] = std::byte{1}; return id; }();
        auto *b_inbound = b.eng.session_for(inbound);
        REQUIRE(b_inbound != nullptr);
        REQUIRE(b.eng.messages().attach_for_fanout(b_inbound->msg_peer(), "topic"));
        REQUIRE(a.eng.messages().attach_for_fanout(a_to_b->msg_peer(), "topic"));
        settle(io);   // drain the subscribe handshake

        b.eng.messages().publish("topic", as_bytes(payload), b_inbound->session_id());
        pump_until(io, [&] { return !a_received.empty(); });
        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == payload);
        REQUIRE(a_to_b->peer_session_id() == b_inbound->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("routing over asio: EAGER dials and completes off note_peer ALONE with no demand call over real TCP",
          "[integration][routing][asio]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/true};
        asio_node b{io, 0xB2, /*eager=*/true};
        const auto id_b = make_id(0xB2);

        // No reach/subscribe/call: awareness ALONE triggers the dial+handshake.
        a.eng.note_peer(id_b, b.listen_ep());
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.session_for(id_b)->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("routing over asio: one node dialing TWO peers near-simultaneously resolves each frame to its OWN source identity over real TCP",
          "[integration][routing][asio]")
{
    // The two-peer receive-path identity leg AND the dial-completion correlation
    // adversary: A dials B and C back-to-back, so both dials are in flight and their
    // TCP connects can complete OUT OF ORDER. The engine must route each completed
    // channel to ITS slot by the dial endpoint. If a channel were mis-correlated,
    // peer-B's published bytes would land in A's C-sink (or vice versa) — exactly the
    // cross-attribution this asserts against. Looped N to expose the race across
    // schedulings; the ctest invocation is re-run >=3 process runs.
    constexpr int k_iterations = 100;
    const std::string from_b = "payload-from-peer-b";
    const std::string from_c = "payload-from-peer-c";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false};
        asio_node c{io, 0xC3, /*eager=*/false};
        const auto id_b = make_id(0xB2);
        const auto id_c = make_id(0xC3);

        a.eng.note_peer(id_b, b.listen_ep());
        a.eng.note_peer(id_c, c.listen_ep());

        // Dial BOTH near-simultaneously: two overlapping in-flight dials.
        a.eng.subscribe(id_b, "topic-b");
        a.eng.subscribe(id_c, "topic-c");
        pump_until(io, [&] { return a.eng.is_connected(id_b) && a.eng.is_connected(id_c); });
        REQUIRE(a.eng.is_connected(id_b));
        REQUIRE(a.eng.is_connected(id_c));

        auto *a_to_b = a.eng.session_for(id_b);
        auto *a_to_c = a.eng.session_for(id_c);
        REQUIRE(a_to_b != nullptr);
        REQUIRE(a_to_c != nullptr);

        // Each slot minted its OWN local epoch at handshake — a mis-correlation
        // would collapse the two slots onto one channel.
        REQUIRE(a_to_b->session_id() != 0);
        REQUIRE(a_to_c->session_id() != 0);

        // Per-slot sinks: B's bytes must reach the B-sink and C's the C-sink.
        std::vector<std::string> a_from_b, a_from_c;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_from_b.emplace_back(to_string(d)); });
        a_to_c->on_message([&](std::string_view, std::span<const std::byte> d) { a_from_c.emplace_back(to_string(d)); });

        const auto inbound1 = [] { auto id = make_id(0x00); id[15] = std::byte{1}; return id; }();
        const auto inbound2 = [] { auto id = make_id(0x00); id[15] = std::byte{2}; return id; }();
        auto *b_in = b.eng.session_for(inbound1);
        auto *c_in = c.eng.session_for(inbound1);   // each remote keys its sole inbound at index 1
        REQUIRE(b_in != nullptr);
        REQUIRE(c_in != nullptr);
        (void)inbound2;

        REQUIRE(b.eng.messages().attach_for_fanout(b_in->msg_peer(), "topic-b"));
        REQUIRE(c.eng.messages().attach_for_fanout(c_in->msg_peer(), "topic-c"));
        REQUIRE(a.eng.messages().attach_for_fanout(a_to_b->msg_peer(), "topic-b"));
        REQUIRE(a.eng.messages().attach_for_fanout(a_to_c->msg_peer(), "topic-c"));
        settle(io);

        b.eng.messages().publish("topic-b", as_bytes(from_b), b_in->session_id());
        c.eng.messages().publish("topic-c", as_bytes(from_c), c_in->session_id());
        pump_until(io, [&] { return !a_from_b.empty() && !a_from_c.empty(); });

        // Each frame resolved to its OWN source-peer identity — no cross-attribution.
        REQUIRE(a_from_b.size() == 1);
        REQUIRE(a_from_c.size() == 1);
        REQUIRE(a_from_b.front() == from_b);
        REQUIRE(a_from_c.front() == from_c);

        // The delivered frames latched each slot's PEER epoch to its own remote: A's
        // B-slot carries B's epoch and the C-slot carries C's. A mis-correlated
        // channel would latch the wrong peer's epoch (or collide both onto one).
        REQUIRE(a_to_b->peer_session_id() == b_in->session_id());
        REQUIRE(a_to_c->peer_session_id() == c_in->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("routing over asio: a dial that completes OUT OF ORDER is correlated to its own slot, not by arrival order",
          "[integration][routing][asio]")
{
    // The deterministic out-of-order adversary. A reaches B and C: B's listener is
    // DOWN at dial time (its dial is refused and the driver retries), while C's is up
    // and C connects IMMEDIATELY. So C's on_dialed fires FIRST — but A enqueued B's
    // slot FIRST (reach(B) before reach(C)). A by-arrival-order (FIFO) tail would pop
    // B's slot for C's channel: B's slot would carry C's connection and C's slot
    // would carry B's — a cross-attribution. The endpoint-correlated tail routes each
    // completed channel to ITS own slot. After C lands, B's listener is brought up on
    // its reserved port and B completes too; each slot must then resolve to its OWN
    // remote epoch over the wire. Looped to vary the scheduler interleavings.
    constexpr int k_iterations = 40;
    const std::string from_b = "out-of-order-from-b";
    const std::string from_c = "out-of-order-from-c";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        asio_node a{io, 0xA1, /*eager=*/false};
        asio_node c{io, 0xC3, /*eager=*/false};
        asio_node b{io, 0xB2, /*eager=*/false, /*listen_now=*/false};
        const auto id_b = make_id(0xB2);
        const auto id_c = make_id(0xC3);

        const auto b_port = reserve_closed_port();   // B's endpoint, listener still DOWN
        a.eng.note_peer(id_b, {"tcp", "127.0.0.1:" + std::to_string(b_port)});
        a.eng.note_peer(id_c, c.listen_ep());

        // Reach B FIRST (enqueued first; its dial is refused → retries) then C (up).
        a.eng.subscribe(id_b, "topic-b");
        a.eng.subscribe(id_c, "topic-c");

        // C completes first while B is still retrying — the out-of-order completion.
        pump_until(io, [&] { return a.eng.is_connected(id_c); });
        REQUIRE(a.eng.is_connected(id_c));
        REQUIRE(!a.eng.is_connected(id_b));   // B has NOT connected yet (listener down)

        // Bring B's listener up on its reserved port; B's retry now succeeds.
        b.listen_on(b_port);
        pump_until(io, [&] { return a.eng.is_connected(id_b); });
        REQUIRE(a.eng.is_connected(id_b));

        auto *a_to_b = a.eng.session_for(id_b);
        auto *a_to_c = a.eng.session_for(id_c);
        REQUIRE(a_to_b != nullptr);
        REQUIRE(a_to_c != nullptr);

        std::vector<std::string> a_from_b, a_from_c;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_from_b.emplace_back(to_string(d)); });
        a_to_c->on_message([&](std::string_view, std::span<const std::byte> d) { a_from_c.emplace_back(to_string(d)); });

        const auto inbound = [] { auto id = make_id(0x00); id[15] = std::byte{1}; return id; }();
        auto *b_in = b.eng.session_for(inbound);
        auto *c_in = c.eng.session_for(inbound);
        REQUIRE(b_in != nullptr);
        REQUIRE(c_in != nullptr);

        REQUIRE(b.eng.messages().attach_for_fanout(b_in->msg_peer(), "topic-b"));
        REQUIRE(c.eng.messages().attach_for_fanout(c_in->msg_peer(), "topic-c"));
        REQUIRE(a.eng.messages().attach_for_fanout(a_to_b->msg_peer(), "topic-b"));
        REQUIRE(a.eng.messages().attach_for_fanout(a_to_c->msg_peer(), "topic-c"));
        settle(io);

        b.eng.messages().publish("topic-b", as_bytes(from_b), b_in->session_id());
        c.eng.messages().publish("topic-c", as_bytes(from_c), c_in->session_id());
        pump_until(io, [&] { return !a_from_b.empty() && !a_from_c.empty(); });

        // No cross-attribution: B's bytes reached the B-slot and C's the C-slot, and
        // each slot latched its OWN remote epoch — even though C's channel completed
        // BEFORE B's despite B's slot being enqueued first.
        REQUIRE(a_from_b.size() == 1);
        REQUIRE(a_from_c.size() == 1);
        REQUIRE(a_from_b.front() == from_b);
        REQUIRE(a_from_c.front() == from_c);
        REQUIRE(a_to_b->peer_session_id() == b_in->session_id());
        REQUIRE(a_to_c->peer_session_id() == c_in->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

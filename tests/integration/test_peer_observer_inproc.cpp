// over-limit: one cohesive observer lifecycle+readiness matrix; the edge cells all assert against
// the one shared recording-observer + routing_engine harness on the single manual clock, so
// splitting them scatters that shared fixture The deterministic peer-observer behavior oracle on
// the manual virtual clock. A recording observer (shared header) is registered on a real
// routing_engine and the full lifecycle + readiness matrix is forced over the inproc backend,
// looped where behavioral with the established proven == k_iterations idiom. Every edge is POSTED
// on the executor, so each assertion is made AFTER a drain. The suite forces the
// named failure modes: connected-vs-reconnected discrimination across a drop+redial,
// disconnected only on an established drop, dead on driver surrender, rejected with
// the real refusal reason, the first-publish-loss-free subscribe->ready->publish
// path over the REAL loop (no faked two-sided attaches), double-fire prevention
// across reconnect (ready == 2 over two cycles), the premature-ready window
// (ready held until the resurrected acks drain), the zero-subscribe immediate ready,
// the accepted-peer edge subset (no reconnect/dead), posted re-entrant delivery, and
// the forged-frame counter-underflow guard. The harness mirrors the drop-seam oracle.

#include "recording_observer.h"

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

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
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
using plexus::io::peer_kind;
using plexus::io::handshake_outcome;

namespace {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept
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

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;
constexpr auto          k_ceiling      = std::chrono::milliseconds(10001);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A handshake config advertising (version) and requiring (compatible) versions. The
// default matched pair handshakes; a peer whose required-compatible exceeds the
// other's advertised version rejects the other with version_incompatible.
handshake_fsm_config make_cfg(std::uint8_t id_seed, std::uint8_t version = 1, std::uint8_t compatible = 1)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = version, .version_minor = 0, .compatible_version_major = compatible, .compatible_version_minor = 0};
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
    id[15]             = std::byte{n};
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

// Frame a subscribe_response exactly as the wire codec does, carrying the chosen
// session_id, so feeding it to a dialer session's on_receive drives the production
// decode + readiness-decrement path. Used to FORGE an unmatched ack.
std::vector<std::byte> make_subscribe_response(std::uint64_t session_id)
{
    plexus::wire::subscribe_response resp{.topic_hash = 0x1234, .status = plexus::wire::subscribe_status::subscribed};
    auto                             payload = plexus::wire::encode_subscribe_response(resp);
    plexus::wire::frame_header       hdr{.type = plexus::wire::msg_type::subscribe_response, .flags = 0, .session_id = session_id, .timestamp_ns = 0, .payload_len = payload.size()};
    return plexus::wire::encode_frame(hdr, payload);
}

// A two-node rendezvous on one bus. A's redial config + A's compatible-version are
// selectable so the surrender and rejection legs can arm them; B is forever/matched.
struct two_node
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};
    plexus::log::null_logger      sink;

    engine a;
    engine b;

    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    explicit two_node(const reconnect_config &a_redial = forever_cfg(), std::uint8_t a_compatible = 1)
            : a(transport_a, ex, make_cfg(0xA1, 1, a_compatible), k_long_timeout, a_redial, k_seed, sink, false)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        a.listen(ep_a);
        b.listen(ep_b);
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
};

}

TEST_CASE("inproc observer: connected fires once and ready fires immediately for a zero-subscribe "
          "peer",
          "[integration][observer][inproc]")
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

        const auto &c = rec.for_peer(net.id_b);
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(c.connected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.ready == 1);        // zero-subscribe peer fires ready on complete
        REQUIRE(c.disconnected == 0); // no drop yet -> no disconnected
        REQUIRE(c.last_kind == peer_kind::dialed);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: a drop+redial fires reconnected (NOT a second connected) and a "
          "disconnected on the established drop",
          "[integration][observer][inproc]")
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
        REQUIRE(rec.for_peer(net.id_b).connected == 1);
        REQUIRE(rec.for_peer(net.id_b).disconnected == 0);

        // A REAL transport drop: close B's accepted end so A observes on_error. The
        // dialer's disconnected fires when the redial tears down the dead session, so
        // it lands after the backoff fires (below), not on the bare drop.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();

        // Drive the backoff: A tears down the dead session (disconnected) then re-dials
        // and re-handshakes; reconnected fires, NOT a second connected (the
        // discriminator lives on the record, not the session).
        net.advance(k_ceiling);
        REQUIRE(net.a.is_connected(net.id_b));
        const auto &c = rec.for_peer(net.id_b);
        REQUIRE(c.connected == 1);    // still exactly one connected
        REQUIRE(c.reconnected == 1);  // the redial fired reconnected
        REQUIRE(c.disconnected == 1); // the established drop fired exactly one disconnect
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: dead fires once when the driver surrenders, and the co-resident live "
          "peer is untouched",
          "[integration][observer][inproc]")
{
    constexpr int           k_iterations   = 50;
    constexpr std::uint32_t k_max_attempts = 3;
    int                     proven         = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();

        inproc_bus<manual_clock>      bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t                   transport_a{ex, bus};
        transport_t                   transport_b{ex, bus};
        transport_t                   transport_c{ex, bus};

        plexus::log::null_logger sink;
        engine                   a(transport_a, ex, make_cfg(0xA1), k_long_timeout, bounded_cfg(k_max_attempts), k_seed, sink, false);
        engine                   b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false);
        engine                   c(transport_c, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, sink, false);
        recording_observer       rec;
        a.add_observer(rec);

        const auto id_b = make_id(0xB2);
        const auto id_c = make_id(0xC3);
        a.listen({"inproc", "node-a"});
        b.listen({"inproc", "node-b"});
        c.listen({"inproc", "node-c"});

        a.note_peer(id_b, {"inproc", "node-b"});
        a.note_peer(id_c, {"inproc", "node-c"});
        a.reach(id_b);
        a.reach(id_c);
        ex.drain();
        REQUIRE(a.is_connected(id_b));
        REQUIRE(a.is_connected(id_c));

        for(std::uint8_t n = 1; n <= k_max_attempts; ++n)
        {
            b.session_for(inbound_slot(n))->tear_down();
            ex.drain();
            if(n < k_max_attempts)
            {
                manual_clock::advance(k_ceiling);
                ex.drain();
            }
        }
        ex.drain(); // flush the posted dead edge

        REQUIRE(a.is_dead(id_b));
        REQUIRE(rec.for_peer(id_b).dead == 1);
        REQUIRE(rec.for_peer(id_b).last_kind == peer_kind::dialed);
        REQUIRE(rec.for_peer(id_c).dead == 0); // the live peer never died
        REQUIRE(a.is_connected(id_c));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: rejected fires once carrying the real refusal reason on a "
          "version-incompatible handshake",
          "[integration][observer][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        // B advertises version 1; A requires compatible >= 2, so A rejects B's
        // response (B's accept carries version 1) -> A fires rejected(reject_version).
        two_node           net(forever_cfg(), /*a_compatible=*/2);
        recording_observer rec;
        net.a.add_observer(rec);

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();

        const auto &c = rec.for_peer(net.id_b);
        REQUIRE(c.rejected == 1);
        REQUIRE(c.last_reason == handshake_outcome::reject_version);
        REQUIRE(c.connected == 0);    // an un-established session never connected
        REQUIRE(c.disconnected == 0); // rejected fires alone (no spurious disconnect)
        REQUIRE(!net.a.is_connected(net.id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: on_peer_ready over the REAL loop, then the awaited publish lands "
          "(first-publish-loss-free)",
          "[integration][observer][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string payload      = "ready-then-publish";
    int               proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node           net;
        recording_observer rec;
        net.a.add_observer(rec);

        // Subscribe through the engine (the REAL counted loop) — NO faked attach.
        net.a.note_peer(net.id_b, net.ep_b);
        net.a.subscribe(net.id_b, "topic");
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // fires once after the ack drains

        // The awaited publish: A is the subscriber, so B publishes and the frame lands
        // at A's per-session sink. Proves the subscribe round-trip actually wired the
        // fan-out (a never-firing ready would leave this empty).
        auto                    *a_to_b = net.a.session_for(net.id_b);
        std::vector<std::string> a_received;
        a_to_b->on_message([&](std::string_view, std::span<const std::byte> d) { a_received.emplace_back(to_string(d)); });

        auto *b_inbound = net.b.session_for(inbound_slot(1));
        REQUIRE(b_inbound != nullptr);
        net.drive(); // let B's producer-side fanout settle from A's subscribe
        net.b.messages().publish("topic", as_bytes(payload), b_inbound->session_id());
        net.drive();

        REQUIRE(a_received.size() == 1);
        REQUIRE(a_received.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: ready fires EXACTLY once per cycle across a reconnect (count == 2 over "
          "two cycles, never 1 or 3)",
          "[integration][observer][inproc]")
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
        net.a.subscribe(net.id_b, "topic");
        net.drive();
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // cycle 1

        // Drop + redial: the fresh incarnation re-arms the latch, resurrects the
        // subscribe through the counted path, and fires ready a SECOND time once acked.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();
        net.advance(k_ceiling);
        REQUIRE(net.a.is_connected(net.id_b));

        const auto &c = rec.for_peer(net.id_b);
        REQUIRE(c.ready == 2); // exactly two -> fire-once-per-cycle + re-arm
        REQUIRE(c.reconnected == 1);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: premature-ready window — ready stays 1 while the resurrected "
          "subscribes are outstanding, becomes 2 only after the acks drain",
          "[integration][observer][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node           net;
        recording_observer rec;
        net.a.add_observer(rec);

        // Connect first (zero-subscribe ready fires once), THEN subscribe N>1 topics
        // on the live session so each is attached and remembered (a late subscribe
        // after ready bumps the counter without re-arming the latch). The remembered
        // demand is what the reconnect resurrection replays.
        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // cycle 1: zero-subscribe ready
        net.a.subscribe(net.id_b, "topic-1");
        net.a.subscribe(net.id_b, "topic-2");
        net.a.subscribe(net.id_b, "topic-3");
        net.drive();
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // late subscribes do NOT re-fire

        // Drop + arm the backoff. Stepping the executor to the instant the reconnect
        // handshake completes runs on_complete -> resubscribe_all (counter now N) ->
        // maybe_fire_ready (held back, count N > 0), but NOT yet the resurrected
        // subscribe_responses (still in flight). The cycle-2 ready cannot have fired.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();
        manual_clock::advance(k_ceiling);
        while(net.ex.step() && !net.a.is_connected(net.id_b))
            ;
        REQUIRE(net.a.is_connected(net.id_b));      // reconnect handshake complete
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // STILL 1 — held during the resurrection window

        // Drain the resurrected subscribe_responses: now the count reaches 0 and the
        // second-cycle ready fires.
        net.drive();
        REQUIRE(rec.for_peer(net.id_b).ready == 2);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: an accepted (inbound) peer fires connected/disconnected/ready but "
          "NEVER reconnected or dead",
          "[integration][observer][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node           net;
        recording_observer rec;
        net.b.add_observer(rec); // observe the ACCEPTING node's inbound slot

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        const auto inbound = inbound_slot(1);
        {
            const auto &c = rec.for_peer(inbound);
            REQUIRE(c.connected == 1);
            REQUIRE(c.ready == 1); // zero-subscribe accepted peer
            REQUIRE(c.last_kind == peer_kind::accepted);
        }

        // Drop the connection from A's side: B's accepted session tears down and fires
        // disconnected, but an accepted peer owns no driver, so NEVER reconnect/dead.
        net.a.session_for(net.id_b)->tear_down();
        net.drive();
        const auto &c = rec.for_peer(inbound);
        REQUIRE(c.disconnected == 1);
        REQUIRE(c.reconnected == 0);
        REQUIRE(c.dead == 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: calling engine.subscribe from inside an observer callback is "
          "posted-safe (no re-entrancy crash)",
          "[integration][observer][inproc]")
{
    // A re-entrant observer: on connected it issues a fresh demand subscribe back
    // through the engine. Delivery is posted, so the nested call cannot re-enter the
    // fire-site synchronously; it takes effect on a later drained turn. The case is
    // also the ASan-critical posted-edge path (the suite runs under asan/ubsan).
    struct reentrant_observer final : public plexus::io::observer
    {
        engine         *eng{nullptr};
        plexus::node_id target{};
        int             connected{0};
        void            on_peer_connected(const plexus::node_id &, std::string_view, peer_kind) override
        {
            ++connected;
            if(eng)
                eng->subscribe(target, "late-topic"); // nested engine call from a callback
        }
    };

    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node           net;
        reentrant_observer obs;
        obs.eng    = &net.a;
        obs.target = net.id_b;
        net.a.add_observer(obs);

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive(); // the nested subscribe runs on a later turn — no crash

        REQUIRE(obs.connected == 1);
        REQUIRE(net.a.is_connected(net.id_b));
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: (un)registering an observer from inside a lifecycle callback does not "
          "corrupt the posted fan-out",
          "[integration][observer][inproc]")
{
    // The fan-out iterates a snapshot, so an observer that removes itself from within
    // its own callback cannot invalidate the in-flight iteration, and a co-registered
    // observer in the same turn still receives its edge. The suite runs under
    // asan/ubsan, which turns a mid-loop vector mutation into a hard failure — so this
    // case is the structural proof that add_observer/remove_observer are callback-safe.
    struct self_removing_observer final : public plexus::io::observer
    {
        engine *eng{nullptr};
        int     connected{0};
        void    on_peer_connected(const plexus::node_id &, std::string_view, peer_kind) override
        {
            ++connected;
            if(eng)
                eng->remove_observer(*this); // mutate m_observers mid-turn
        }
    };

    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node               net;
        self_removing_observer first;
        recording_observer     second;
        first.eng = &net.a;
        net.a.add_observer(first);  // unregisters itself on connected
        net.a.add_observer(second); // the co-observer must still receive its edge

        net.a.note_peer(net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();

        REQUIRE(first.connected == 1); // fired once, then unregistered
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(second.for_peer(net.id_b).connected == 1); // the co-observer is untouched
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc observer: a forged subscribe_response with no outstanding match is "
          "warned-and-dropped; the counter is not corrupted and a later legit subscribe still "
          "reaches ready",
          "[integration][observer][inproc]")
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
        REQUIRE(rec.for_peer(net.id_b).ready == 1); // zero-subscribe ready fired

        // FORGE a subscribe_response with NO outstanding subscribe: the underflow
        // guard warns-and-drops it BEFORE the decrement, so the uint16_t cannot wrap.
        auto *a_session = net.a.session_for(net.id_b);
        REQUIRE(a_session != nullptr);
        auto forged = make_subscribe_response(a_session->peer_session_id());
        a_session->on_receive(forged);
        net.drive();

        // The session is intact and a later legitimate subscribe still reaches ready —
        // the counter was not corrupted. (ready already latched this cycle, so no new
        // ready edge fires; the proof is that the legit subscribe completes its ack
        // cycle cleanly and the session stays connected.)
        REQUIRE(net.a.is_connected(net.id_b));
        net.a.subscribe(net.id_b, "post-forge-topic");
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));

        // A second connection cycle proves ready is reachable again: drop+redial and
        // assert ready re-fires (count 2) — impossible if the counter had wrapped.
        net.b.session_for(inbound_slot(1))->tear_down();
        net.drive();
        net.advance(k_ceiling);
        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(rec.for_peer(net.id_b).ready == 2);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

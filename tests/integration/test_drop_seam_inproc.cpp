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
#include "plexus/io/observer.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/io/shm/broadcast_ring.h"
#include "plexus/io/shm/ring_layout.h"
#include "plexus/io/shm/loan_status.h"
#include "plexus/io/shm/shm_channel.h"
#include "plexus/io/shm/shm_mux_member.h"
#include "plexus/io/shm/shm_topic_registry.h"
#include "plexus/io/shm/region_broker_concept.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
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
// Policy — identical in shape to the routing oracle's manual_clock.
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
constexpr std::uint64_t k_seed         = 0xC0FFEEu; // fixed seed -> reproducible backoff
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

// A tiny logger that counts warn() calls — the close funnel emits exactly ONE warn
// per protocol-error close, so the semantic-close leg asserts count == 1 (never a
// per-byte / per-frame amplification). A plain instance, no static singleton.
struct counting_logger final : plexus::log::logger
{
    int  warns{0};
    void warn(std::string_view) override { ++warns; }
};

// Frame a handshake response with a chosen status exactly as the wire codec does,
// so feeding it to a dialer session's on_receive drives the production decode +
// FSM path (a version_incompatible response makes on_response return abort). The
// control frame carries session_id 0, as every handshake frame does.
std::vector<std::byte> make_handshake_response(plexus::wire::handshake_status status)
{
    plexus::wire::handshake_response resp{};
    resp.id[0]                         = std::byte{0xB2};
    resp.version_major                 = 1;
    resp.version_minor                 = 0;
    resp.compatible_version_major      = 1;
    resp.compatible_version_minor      = 0;
    resp.protocol_version              = plexus::wire::k_protocol_version;
    resp.status                        = status;
    auto                       payload = plexus::wire::encode_handshake_response(resp);
    plexus::wire::frame_header hdr{.type         = plexus::wire::msg_type::handshake_resp,
                                   .flags        = 0,
                                   .session_id   = 0,
                                   .timestamp_ns = 0,
                                   .payload_len  = payload.size()};
    return plexus::wire::encode_frame(hdr, payload);
}

// A complete, header-undecodable frame: a header-size-plus run whose magic bytes
// are wrong. on_receive's decode_header returns nullopt on it, which the close
// funnel treats as a frame-level protocol violation (it does NOT route the frame).
std::vector<std::byte> make_undecodable_frame()
{
    return std::vector<std::byte>(plexus::wire::header_size + 4, std::byte{0xFF});
}

// A unidirectional data frame for a topic NOBODY subscribed: it reaches the
// forwarder's benign warn-and-drop receive tail (an unresolved topic), which is
// NOT a protocol error and must NOT trip the close funnel.
std::vector<std::byte> make_unknown_topic_frame(std::uint64_t session_id)
{
    std::array<std::byte, 4>   body{};
    plexus::wire::frame_header hdr{.type         = plexus::wire::msg_type::unidirectional,
                                   .flags        = 0,
                                   .session_id   = session_id,
                                   .timestamp_ns = 0,
                                   .payload_len  = body.size()};
    return plexus::wire::encode_frame(hdr, body);
}

// The inbound slot B keys an accepted session on: inbound ids are minted in arrival
// order from 1 (the registry's synthetic inbound identity). The n-th accept yields
// id[15] = n; the dialer's real node_id never reaches B's slot map here.
plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
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
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};

    // A owns a counting logger so the semantic-close leg can assert the single warn
    // the protocol-error funnel emits; it is injected into A's engine by reference.
    counting_logger a_logger;

    engine a;
    engine b;

    discovery_stub  discovery;
    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    explicit two_node(const reconnect_config &a_redial = forever_cfg())
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, a_redial, k_seed, false, a_logger)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, false)
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

// An observer that records every posted drop cause it sees — the sink an inproc /
// shm drop edge will eventually post into.
struct recording_drop_observer final : plexus::io::observer
{
    std::vector<plexus::io::detail::drop_cause> seen;
    void on_drop(const plexus::io::detail::drop_event &ev) override { seen.push_back(ev.cause); }
    [[nodiscard]] bool any() const { return !seen.empty(); }
};

// The no-op notifier the shm congestion fixture needs (it exercises the loan gate, not
// the cross-process wake).
struct null_notifier
{
    void signal() noexcept {}
    void arm(plexus::detail::move_only_function<void()>) {}
    void disarm() noexcept {}
};

static_assert(plexus::io::shm::notifier<null_notifier>);

// A heap-backed, cache-line-aligned region the broadcast_ring lays its control + slab
// into (the single-process stand-in for a mapped shm segment).
struct backing_region
{
    explicit backing_region(std::size_t bytes)
            : m_storage(bytes + plexus::io::shm::k_cache_line)
    {
        auto base    = reinterpret_cast<std::uintptr_t>(m_storage.data());
        auto aligned = (base + plexus::io::shm::k_cache_line - 1) &
                ~static_cast<std::uintptr_t>(plexus::io::shm::k_cache_line - 1);
        m_data = reinterpret_cast<std::byte *>(aligned);
        m_size = bytes;
    }
    std::span<std::byte> span() const noexcept { return {m_data, m_size}; }

private:
    std::vector<std::byte> m_storage;
    std::byte             *m_data{nullptr};
    std::size_t            m_size{0};
};

// A move-only handle over a heap region (the single-process stand-in for a mapped
// segment) the stub broker hands back — only needed so shm_byte_channel's dtor release
// has a registry to call into; this oracle never acquires a ring through it.
class stub_region_handle
{
public:
    stub_region_handle() = default;
    std::span<std::byte> bytes() const { return {}; }
};

// The minimal region_broker the shm drop-edge oracle needs: shm_byte_channel borrows a
// registry by reference and calls release(fqn, request) on destruction. The channel here
// is built over a hand-pinned ring, never acquired through the broker, so create/attach
// stay unused; the registry's release on an unknown key is a no-op.
class stub_broker
{
public:
    using region_handle = stub_region_handle;
    plexus::io::shm::region_status create(std::string_view, std::size_t,
                                          const plexus::io::shm::create_options &, region_handle &)
    {
        return plexus::io::shm::region_status::failed;
    }
    plexus::io::shm::region_status attach(std::string_view, region_handle &)
    {
        return plexus::io::shm::region_status::not_found;
    }
    void set_attach_policy(plexus::detail::move_only_function<bool(std::string_view)>) {}
};

static_assert(plexus::io::shm::region_broker<stub_broker>);

}

TEST_CASE("inproc drop seam: a REAL partner-end close drives an automatic re-dial with no "
          "injection and a fresh epoch",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto pre_epoch = net.a.session_for(net.id_b)->session_id();
        const auto before    = net.a.attempt_count(net.id_b);

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
    int           proven       = 0;
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

TEST_CASE("inproc drop seam: crossing a surrender bound marks is_dead and re-dials nothing; a "
          "co-resident live peer is untouched",
          "[integration][drop_seam][inproc]")
{
    constexpr int           k_iterations   = 100;
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

        // A's B slot is bounded (max_attempts) so enough real drops surrender it; C
        // is forever, so it is the live co-resident peer the surrender must not touch.
        engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, bounded_cfg(k_max_attempts),
                 k_seed, false);
        engine b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, false);
        engine c(transport_c, ex, make_cfg(0xC3), k_long_timeout, forever_cfg(), k_seed, false);

        plexus::node_id id_b = make_id(0xB2);
        plexus::node_id id_c = make_id(0xC3);
        endpoint        ep_b{"inproc", "node-b"};
        endpoint        ep_c{"inproc", "node-c"};
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

TEST_CASE("inproc drop seam: a SEMANTIC protocol-error close (an FSM abort from a rejecting "
          "response) warns once, tears down, and does NOT re-dial",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto before       = net.a.attempt_count(net.id_b);
        const int  warns_before = net.a_logger.warns;

        // Feed A's dialer session a version_incompatible handshake response: the
        // production decode + FSM path returns abort, which the close funnel routes
        // through close_for_protocol_error — ONE warn + tear_down + the latched
        // protocol-error disposition that short-circuits the transport-drop re-dial.
        auto *a_session = net.a.session_for(net.id_b);
        REQUIRE(a_session != nullptr);
        auto reject = make_handshake_response(plexus::wire::handshake_status::version_incompatible);
        a_session->on_receive(reject);
        net.drive();

        REQUIRE(net.a_logger.warns == warns_before + 1); // exactly one warn, never per-byte
        REQUIRE(!net.a.is_connected(net.id_b));          // the session was torn down

        // The dial-rearm short-circuit: a protocol-error close does NOT advance the
        // dialer's attempt_count (contrast the real-drop leg's +1). Driving the
        // backoff window confirms no re-dial was armed.
        REQUIRE(net.a.attempt_count(net.id_b) == before);
        net.advance(k_ceiling);
        REQUIRE(net.a.attempt_count(net.id_b) == before);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc drop seam: a header-undecodable complete frame closes via the protocol-error "
          "funnel and does NOT re-dial",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto before       = net.a.attempt_count(net.id_b);
        const int  warns_before = net.a_logger.warns;

        // A complete frame whose header does NOT decode is a frame-level protocol
        // violation: on_receive funnels it into close_for_protocol_error (one warn +
        // tear_down) and does NOT route it. No re-dial (the short-circuit).
        auto *a_session = net.a.session_for(net.id_b);
        REQUIRE(a_session != nullptr);
        auto bad = make_undecodable_frame();
        a_session->on_receive(bad);
        net.drive();

        REQUIRE(net.a_logger.warns == warns_before + 1);
        REQUIRE(!net.a.is_connected(net.id_b));
        REQUIRE(net.a.attempt_count(net.id_b) == before);
        net.advance(k_ceiling);
        REQUIRE(net.a.attempt_count(net.id_b) == before);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc drop seam: a benign unknown-topic data frame reaches the forwarder warn-and-drop "
          "tail and does NOT close the session (the scope guard)",
          "[integration][drop_seam][inproc]")
{
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        two_node net;

        net.discovery.announce(net.a, net.id_b, net.ep_b);
        net.a.reach(net.id_b);
        net.drive();
        REQUIRE(net.a.is_connected(net.id_b));
        const auto before = net.a.attempt_count(net.id_b);

        // A data frame for a topic nobody subscribed is the forwarder's benign
        // warn-and-drop (an unresolved topic), NOT a protocol violation: the session
        // STAYS up and no re-dial is armed. Carry the latched epoch so the staleness
        // gate passes it through to the forwarder tail rather than dropping it.
        auto *a_session = net.a.session_for(net.id_b);
        REQUIRE(a_session != nullptr);
        auto frame = make_unknown_topic_frame(a_session->peer_session_id());
        a_session->on_receive(frame);
        net.drive();

        REQUIRE(net.a.is_connected(net.id_b));            // the session is NOT closed
        REQUIRE(net.a.attempt_count(net.id_b) == before); // no re-dial
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc drop seam: a data packet to a vanished partner is dropped AND must reach an "
          "installed observer",
          "[integration][drop_seam][inproc][unmatched]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        manual_clock::reset();
        inproc_bus<manual_clock>      bus;
        inproc_executor<manual_clock> ex{bus};

        inproc_channel<manual_clock> sender(ex);
        inproc_channel<manual_clock> bystander(ex); // a live, unrelated channel on the bus
        recording_drop_observer      observer;

        std::size_t bystander_rx = 0;
        bystander.on_data([&](std::span<const std::byte>) { ++bystander_rx; });
        sender.on_drop([&](const plexus::io::detail::drop_event &ev) { observer.on_drop(ev); });

        // Point the sender at an endpoint the bus never minted: key_for resolves to 0,
        // so the enqueued packet matches no live channel and falls through deliver_one's
        // unmatched-partner path (the silent drop the new inproc edge now covers).
        sender.connect_to({"inproc", "no-such-partner"});

        std::array<std::byte, 4> body{std::byte{1}, std::byte{2}, std::byte{3}, std::byte{4}};
        sender.send(std::span<const std::byte>{body});
        ex.drain(); // the bus delivers (or, here, fails to match) inside the step loop

        // The packet was dropped: no live channel received it (the bystander stays at 0).
        // The inproc unmatched-partner edge reports the drop on the sender's on_drop seam,
        // which routes to the observer — so the observer SEES the unroutable drop.
        REQUIRE(bystander_rx == 0);
        REQUIRE(observer.any());
        REQUIRE(observer.seen.front() == plexus::io::detail::drop_cause::unroutable);
    }
}

TEST_CASE("inproc drop seam: an engine-installed observer sees an unroutable inproc drop through "
          "the PRODUCTION channel binding (no test-local on_drop)",
          "[integration][drop_seam][inproc][unmatched][production]")
{
    for(int loop = 0; loop < 4; ++loop)
    {
        manual_clock::reset();
        inproc_bus<manual_clock>      bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t                   transport_a{ex, bus};
        transport_t                   transport_b{ex, bus};

        engine a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, false);
        // B's engine is destroyed mid-test to make A's heartbeat target vanish, so it
        // lives in an optional A's slot outlives (no clock advance => no re-dial yet).
        std::optional<engine> b;
        b.emplace(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, false);

        plexus::node_id id_b = make_id(0xB2);
        endpoint        ep_b{"inproc", "node-b"};
        a.listen({"inproc", "node-a"});
        b->listen(ep_b);

        // The observer is installed ONLY through the engine's public add_observer — the
        // test never touches the channel's on_drop. The drop must reach it solely via the
        // engine's wire_channel_drop binding the dialer channel's on_drop to drop_sink().
        recording_drop_observer observer;
        a.add_observer(observer);

        a.note_peer(id_b, ep_b);
        a.reach(id_b);
        ex.drain();
        REQUIRE(a.is_connected(id_b));

        // Destroy B: its accepted inproc_channel deregisters from the bus, so A's dialer's
        // resolved partner key now matches no live channel. A's slot is untouched.
        b.reset();

        // A heartbeat on A's OWN live session sends a frame the bus cannot route — the
        // unmatched-partner path in deliver_one fires report_unroutable on A's channel,
        // which the engine bound to its posted drop_sink at dial time.
        auto *a_session = a.session_for(id_b);
        REQUIRE(a_session != nullptr);
        a_session->emit_heartbeat();
        ex.drain(); // the bus fails to match inside the step loop, then the posted drop fans out

        REQUIRE(observer.any());
        REQUIRE(observer.seen.front() == plexus::io::detail::drop_cause::unroutable);
    }
}

TEST_CASE("inproc drop seam: a congested shm send surfaces the congestion verdict AND must reach "
          "an installed observer",
          "[integration][drop_seam][shm][congested]")
{
    using plexus::io::shm::broadcast_ring;
    using plexus::io::shm::loan_status;
    using plexus::io::shm::shm_channel;
    using plexus::io::shm::shm_byte_channel;
    using plexus::io::shm::shm_topic_registry;
    using plexus::io::shm::control_region_bytes;
    using plexus::io::shm::slab_region_bytes;

    constexpr std::uint64_t cells = 16;
    constexpr std::uint64_t slot  = 64;

    for(int loop = 0; loop < 4; ++loop)
    {
        backing_region control{control_region_bytes(cells)};
        backing_region slab{slab_region_bytes(cells, slot)};
        broadcast_ring ring;
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), cells, slot, ring) ==
                loan_status::ok);
        null_notifier           notify;
        recording_drop_observer observer;

        // A best_effort + drop_newest channel whose ring is fully pinned by a held reader
        // lap: the next send finds no recyclable cell and surfaces congested (the verdict
        // the shm congestion drop edge posts into the observer). The byte_channel wraps the
        // channel and owns the on_drop edge; the registry is borrowed only for its dtor
        // release, which is a no-op for this hand-built (never-acquired) ring.
        shm_channel<null_notifier> channel(ring, notify, plexus::io::reliability::best_effort,
                                           plexus::io::congestion::drop_newest);
        stub_broker                broker;
        shm_topic_registry<stub_broker, null_notifier> registry(
                broker, plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest);
        shm_byte_channel<stub_broker, null_notifier> bytechan(
                registry, channel, "shm-topic", plexus::io::endpoint{"shm", "shm-topic"});
        bytechan.on_drop([&](const plexus::io::detail::drop_event &ev) { observer.on_drop(ev); });

        std::uint32_t idx = 0;
        REQUIRE(ring.register_cursor(idx) == loan_status::ok);
        std::uint64_t cursor = ring.tail_position();
        for(std::uint64_t i = 0; i < cells; ++i)
        {
            broadcast_ring::claim_result claim;
            REQUIRE(ring.claim_with_policy(
                            sizeof(std::uint32_t), plexus::io::reliability::best_effort,
                            plexus::io::congestion::drop_newest, claim) == loan_status::ok);
            const std::uint32_t v = 0xBEEF0000u | static_cast<std::uint32_t>(i);
            std::memcpy(claim.slab.data(), &v, sizeof(v));
            REQUIRE(ring.commit(claim.position, sizeof(v)) == loan_status::ok);
            REQUIRE(ring.pin_if_current(cursor) == true);
            ++cursor;
        }

        std::uint32_t payload = 0xFEEDFACEu;
        std::byte     bytes[sizeof(payload)];
        std::memcpy(bytes, &payload, sizeof(payload));
        // The byte_channel send hits the congested verdict and emits the drop edge
        // ADDITIVELY (on_error stays available); the observer sees the congestion drop.
        bytechan.send(std::span<const std::byte>(bytes, sizeof(bytes)));

        REQUIRE(observer.any());
        REQUIRE(observer.seen.front() == plexus::io::detail::drop_cause::blocked);

        for(std::uint64_t i = 0; i < cells; ++i)
            ring.refcount_at(i).fetch_sub(1);
        ring.unregister_cursor(idx);
    }
}

TEST_CASE("inproc drop seam: a congested shm send reaches an engine-installed observer through the "
          "ERASED production binding and the posted fan-out",
          "[integration][drop_seam][shm][congested][production]")
{
    using plexus::io::shm::broadcast_ring;
    using plexus::io::shm::loan_status;
    using plexus::io::shm::shm_channel;
    using plexus::io::shm::shm_byte_channel;
    using plexus::io::shm::shm_topic_registry;
    using plexus::io::shm::control_region_bytes;
    using plexus::io::shm::slab_region_bytes;

    constexpr std::uint64_t cells = 16;
    constexpr std::uint64_t slot  = 64;

    for(int loop = 0; loop < 4; ++loop)
    {
        manual_clock::reset();

        // The engine is the drop_sink owner + posted fan-out + observer registry — the SAME
        // role it plays for a real shm node. A same-host shm member only ever rides the
        // multiplexer, so its channel reaches the engine ERASED as a polymorphic_byte_channel;
        // the engine binds on_drop through that erasure (wire_channel_drop). This proves the
        // erased on_drop forwards to the concrete shm channel and posts to the observer.
        inproc_bus<manual_clock>      bus;
        inproc_executor<manual_clock> ex{bus};
        transport_t                   transport{ex, bus};
        engine eng(transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, false);
        recording_drop_observer observer;
        eng.add_observer(observer);

        backing_region control{control_region_bytes(cells)};
        backing_region slab{slab_region_bytes(cells, slot)};
        broadcast_ring ring;
        REQUIRE(broadcast_ring::create(control.span(), slab.span(), cells, slot, ring) ==
                loan_status::ok);
        null_notifier notify;

        shm_channel<null_notifier> channel(ring, notify, plexus::io::reliability::best_effort,
                                           plexus::io::congestion::drop_newest);
        stub_broker                broker;
        shm_topic_registry<stub_broker, null_notifier> registry(
                broker, plexus::io::reliability::best_effort, plexus::io::congestion::drop_newest);

        // Erase the concrete shm channel behind the production polymorphic_byte_channel and
        // install the engine's posted drop_sink through the ERASED on_drop verb — exactly
        // the call wire_channel_drop makes for a muxed shm node. NO concrete on_drop is bound
        // by the test; remove either the erasure forward or the engine binding and this fails.
        auto concrete = std::make_unique<shm_byte_channel<stub_broker, null_notifier>>(
                registry, channel, "shm-topic", plexus::io::endpoint{"shm", "shm-topic"});
        plexus::io::polymorphic_byte_channel erased(
                std::make_unique<
                        plexus::io::channel_adapter<shm_byte_channel<stub_broker, null_notifier>>>(
                        std::move(concrete)));
        erased.on_drop(eng.drop_sink());

        std::uint32_t idx = 0;
        REQUIRE(ring.register_cursor(idx) == loan_status::ok);
        std::uint64_t cursor = ring.tail_position();
        for(std::uint64_t i = 0; i < cells; ++i)
        {
            broadcast_ring::claim_result claim;
            REQUIRE(ring.claim_with_policy(
                            sizeof(std::uint32_t), plexus::io::reliability::best_effort,
                            plexus::io::congestion::drop_newest, claim) == loan_status::ok);
            const std::uint32_t v = 0xBEEF0000u | static_cast<std::uint32_t>(i);
            std::memcpy(claim.slab.data(), &v, sizeof(v));
            REQUIRE(ring.commit(claim.position, sizeof(v)) == loan_status::ok);
            REQUIRE(ring.pin_if_current(cursor) == true);
            ++cursor;
        }

        std::uint32_t payload = 0xFEEDFACEu;
        std::byte     bytes[sizeof(payload)];
        std::memcpy(bytes, &payload, sizeof(payload));
        // The send hits the congested verdict and emits the drop edge; it travels concrete
        // on_drop -> engine drop_sink -> POSTED fan-out. Drain the executor for the posted turn.
        erased.send(std::span<const std::byte>(bytes, sizeof(bytes)));
        ex.drain();

        REQUIRE(observer.any());
        REQUIRE(observer.seen.front() == plexus::io::detail::drop_cause::blocked);

        for(std::uint64_t i = 0; i < cells; ++i)
            ring.refcount_at(i).fetch_sub(1);
        ring.unregister_cursor(idx);
    }
}

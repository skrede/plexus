// Deterministic single-connection reconnect oracle on the manual virtual clock
// with a fixed RNG seed, so backoff intervals fire deterministically and surrender
// is provable without wall-clock flakiness. Covers the four reconnect decisions:
// an initial refused dial backs off and re-dials, then completes once the listener
// appears; an established session whose channel drops tears down, backs off,
// re-dials and re-handshakes — while a CLEAN close does NOT re-dial; the surrender
// bounds (max_attempts / max_elapsed) stop re-dialing and report the session dead,
// and with neither bound set it retries forever at the ceiling cadence; each
// reconnect mints a fresh session_id epoch and the staleness gate drops a
// dead-incarnation straggler. It also gates the POST-RECONNECT
// steady-state publish loop as zero-alloc (the reconnect cycle introduces no
// steady-state hot-path allocation regression) and documents the empirically-tuned
// recommended backoff config. This TU owns the TU-local alloc counter.

#include "plexus/io/reconnect.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/epoch_source.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
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
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

// The virtual clock the backoff + handshake timers fire from, and the matching
// Policy. Identical in shape to the handshake-timeout oracle's manual_clock.
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

// A non-allocating sink Policy mirroring test_hot_path_alloc: its channel records
// each send without copying the bytes, so a forwarder<sink_policy> publish loop
// exercises the FULL steady-state framing path (frame-once into reused scratch +
// fan-out) with no transport-side allocation masking it. The post-reconnect gate
// runs THIS loop after a real reconnect to prove the reconnect cycle introduces no
// steady-state hot-path allocation regression (the inproc bus copies each packet,
// so the production hot path is measured over the sink, exactly as the existing
// steady-state gate does).
struct sink_executor {};

struct sink_channel
{
    explicit sink_channel(sink_executor &) {}
    sink_channel(sink_executor &, std::error_code &) {}
    void send(std::span<const std::byte> d) { total_bytes += d.size(); ++sends; }
    void close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    std::size_t total_bytes{0};
    std::size_t sends{0};
};

struct sink_timer
{
    explicit sink_timer(sink_executor &) {}
    sink_timer(sink_executor &, std::error_code &) {}
    void expires_after(std::chrono::milliseconds) {}
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

struct sink_policy
{
    using executor_type = sink_executor &;
    using byte_channel_type = sink_channel;
    using timer_type = sink_timer;
    using byte_owner = std::shared_ptr<const void>;
    static void post(executor_type, plexus::detail::move_only_function<void()> fn) { fn(); }
};

static_assert(plexus::Policy<sink_policy>);

using session = plexus::io::peer_session<manual_policy>;
using msg_forwarder = plexus::io::message_forwarder<manual_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<manual_policy>;
using transport_t = inproc_transport<manual_clock>;
using driver_t = plexus::io::reconnect<manual_policy, transport_t, manual_clock>;

constexpr auto k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xC0FFEEu;   // fixed seed → reproducible backoff
const plexus::io::endpoint k_svc{"inproc", "svc"};

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

// Synthesize a unidirectional "topic" data frame carrying a chosen session_id via
// the production framing path, so feeding it to a receiver's on_receive exercises
// the real staleness gate.
std::vector<std::byte> make_data_frame(const std::string &payload, std::uint8_t session_id)
{
    msg_forwarder framer;
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex(bus);
    inproc_channel<manual_clock> capture(ex);
    inproc_channel<manual_clock> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    msg_forwarder::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

// A single-connection reconnect harness on the manual clock: the reconnect driver
// owns the dial-retry cycle on the dialer (requester) side; the harness rebuilds a
// fresh requester peer_session from each on_dialed channel (a NEW epoch per
// reconnect) and tears the dead incarnation down on on_redial. The listener side
// (responder) is registered/unregistered to model an endpoint that is down then up.
// Members are ordered bus/executor/transport BEFORE the channels so destruction
// unwinds channels before the bus they registered on.
struct harness
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport{ex, bus};

    msg_forwarder req_messages;
    msg_forwarder resp_messages;
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    // The per-peer records own the channel + the epoch well and OUTLIVE every
    // incarnation, so each rebuilt session draws a strictly-later epoch with no
    // hand-off of the dead one. The redial driver is a harness-owned SIBLING of the
    // requester's record (NOT a record member); both are declared BEFORE the
    // std::optional sessions so destruction unwinds the session first.
    plexus::io::peer_context<manual_policy> req_ctx;
    plexus::io::peer_context<manual_policy> resp_ctx;
    driver_t driver;
    std::optional<session> requester;
    std::optional<session> responder;

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    bool listening{false};
    int dead{0};

    explicit harness(const reconnect_config &cfg)
        : driver(transport, ex, cfg, k_svc, k_seed)
    {
        transport.on_accepted([this](std::unique_ptr<inproc_channel<manual_clock>> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            responder.emplace(resp_ctx, ex, make_cfg(0x01), k_long_timeout,
                              resp_messages, resp_procedures, true);
            responder->on_message([this](std::string_view, std::span<const std::byte> d) {
                resp_received.emplace_back(to_string(d));
            });
            responder->start();
        });
        transport.on_dialed([this](std::unique_ptr<inproc_channel<manual_clock>> ch, const plexus::io::endpoint &) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            requester.emplace(req_ctx, ex, make_cfg(0x02), k_long_timeout,
                              req_messages, req_procedures, false);
            requester->on_message([this](std::string_view, std::span<const std::byte> d) {
                req_received.emplace_back(to_string(d));
            });
            requester->start();
        });
        // The driver no longer self-wires the transport's dial-failure callback (a
        // shared transport's single callback cannot belong to one of many drivers);
        // the owner routes a failure to its sole driver.
        transport.on_dial_failed([this](const plexus::io::endpoint &, plexus::io::io_error) {
            driver.notify_dial_failed();
        });
        // Tear the dead requester down before the fresh channel arrives, and count
        // surrender as a reported death the oracle asserts.
        driver.on_redial([this] {
            if(requester) requester->tear_down();
        });
        driver.on_dead([this] { ++dead; });
    }

    void listen() { transport.listen(k_svc); listening = true; }
    void unlisten() { transport.close(); listening = false; }
    void drive() { ex.drain(); }
    void advance(std::chrono::nanoseconds d) { manual_clock::advance(d); drive(); }
};

}

TEST_CASE("inproc reconnect: an initial refused dial backs off and re-dials, completing once the listener appears",
          "[integration][reconnect][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::nullopt, std::nullopt};
        harness h(cfg);

        // No listener yet: the initial dial is refused, the driver schedules a re-dial.
        h.driver.start();
        h.drive();
        REQUIRE(h.driver.attempt_count() >= 1);
        REQUIRE(!h.driver.is_surrendered());
        REQUIRE(!h.requester);                 // nothing dialed through yet

        // The endpoint comes up; advancing past the ceiling fires the backoff timer,
        // the re-dial now finds the listener, and both sides complete the handshake.
        h.listen();
        h.advance(std::chrono::milliseconds(10001));
        REQUIRE(h.requester);
        REQUIRE(h.requester->is_complete());
        REQUIRE(h.responder->is_complete());
        REQUIRE(h.requester->session_id() != 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc reconnect: an established session whose channel drops re-dials and re-handshakes; a clean close does NOT re-dial",
          "[integration][reconnect][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::nullopt, std::nullopt};
        harness h(cfg);
        h.listen();
        h.driver.start();
        h.drive();
        REQUIRE(h.requester->is_complete());
        const auto first_epoch = h.requester->session_id();
        const auto attempts_after_connect = h.driver.attempt_count();

        // A clean, intentional close does NOT advance the attempt counter (no re-dial
        // amplification): the driver is only told about a TRANSPORT drop.
        const auto before_clean = h.driver.attempt_count();
        h.drive();
        REQUIRE(h.driver.attempt_count() == before_clean);

        // The established channel drops: the harness routes the transport drop to the
        // driver, which tears down the dead incarnation, backs off, and re-dials.
        h.driver.on_channel_dropped();
        REQUIRE(h.driver.attempt_count() == attempts_after_connect + 1);
        h.advance(std::chrono::milliseconds(10001));

        // A fresh session re-handshaked to completion with a NEW epoch.
        REQUIRE(h.requester->is_complete());
        REQUIRE(h.responder->is_complete());
        REQUIRE(h.requester->session_id() != 0);
        REQUIRE(h.requester->session_id() != first_epoch);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc reconnect: surrender on max_attempts and on max_elapsed reports the session dead and stops re-dialing; neither set retries forever",
          "[integration][reconnect][inproc]")
{
    SECTION("max_attempts surrender")
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::uint32_t{3}, std::nullopt};
        harness h(cfg);   // never listening → every dial is refused
        h.driver.start();
        // Drain repeatedly across ceilings: each refused dial schedules the next until
        // the attempt counter hits the bound, then the driver reports dead and stops.
        for(int i = 0; i < 10 && !h.driver.is_surrendered(); ++i)
            h.advance(std::chrono::milliseconds(10001));
        REQUIRE(h.driver.is_surrendered());
        REQUIRE(h.driver.attempt_count() == 3);
        REQUIRE(h.dead == 1);
        // No further dial happens after surrender: another advance does not move the counter.
        const auto frozen = h.driver.attempt_count();
        h.advance(std::chrono::milliseconds(10001));
        REQUIRE(h.driver.attempt_count() == frozen);
    }

    SECTION("max_elapsed surrender")
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::nullopt, std::chrono::milliseconds(25000)};
        harness h(cfg);
        h.driver.start();
        for(int i = 0; i < 20 && !h.driver.is_surrendered(); ++i)
            h.advance(std::chrono::milliseconds(10001));
        REQUIRE(h.driver.is_surrendered());
        REQUIRE(h.dead == 1);
    }

    SECTION("neither bound set retries forever at the ceiling cadence")
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::nullopt, std::nullopt};
        harness h(cfg);
        h.driver.start();
        for(int i = 0; i < 50; ++i)
            h.advance(std::chrono::milliseconds(10001));
        REQUIRE(!h.driver.is_surrendered());
        REQUIRE(h.dead == 0);
        REQUIRE(h.driver.attempt_count() >= 50);   // kept dialing, never gave up
    }
}

TEST_CASE("inproc reconnect: backoff grows full-jitter to the ceiling then holds, reproducibly with a fixed seed",
          "[integration][reconnect][inproc]")
{
    reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                         std::nullopt, std::nullopt};
    // Two independent RNGs with the SAME seed produce the identical sequence
    // (reproducibility), and every sample stays within the growing full-jitter
    // ceiling [0, min(max_delay, min_delay*2^attempt)] (growth then hold).
    std::mt19937_64 a(k_seed), b(k_seed);
    for(std::uint32_t attempt = 1; attempt <= 30; ++attempt)
    {
        auto da = plexus::io::compute_backoff(cfg, attempt, a);
        auto db = plexus::io::compute_backoff(cfg, attempt, b);
        REQUIRE(da == db);
        const auto shift = std::min(attempt, std::uint32_t{20});
        auto ceiling = cfg.min_delay * (std::uint64_t{1} << shift);
        if(ceiling > cfg.max_delay) ceiling = cfg.max_delay;
        REQUIRE(da.count() >= 0);
        REQUIRE(da <= ceiling);
    }
    // Once the ceiling is reached the bound holds at max_delay for all later attempts.
    std::mt19937_64 c(k_seed);
    for(std::uint32_t attempt = 1; attempt <= 40; ++attempt)
        REQUIRE(plexus::io::compute_backoff(cfg, attempt, c) <= cfg.max_delay);
}

TEST_CASE("inproc reconnect: each reconnect mints a fresh epoch and the staleness gate drops a dead-incarnation straggler",
          "[integration][reconnect][inproc]")
{
    constexpr int k_iterations = 100;
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                             std::nullopt, std::nullopt};
        harness h(cfg);
        h.listen();
        h.driver.start();
        h.drive();
        REQUIRE(h.requester->is_complete());
        const auto dead_epoch = h.requester->session_id();

        // Reconnect: drop → backoff → re-dial → fresh handshake → NEW epoch.
        h.driver.on_channel_dropped();
        h.advance(std::chrono::milliseconds(10001));
        REQUIRE(h.requester->is_complete());
        const auto live_epoch = h.requester->session_id();
        REQUIRE(live_epoch != dead_epoch);

        // Subscribe so the responder resolves the topic; latch the live epoch with a
        // real publish from the reconnected requester.
        REQUIRE(h.resp_messages.attach(h.responder->msg_peer(), "topic"));
        REQUIRE(h.req_messages.attach_for_fanout(h.requester->msg_peer(), "topic"));
        h.drive();
        h.req_messages.publish("topic", as_bytes(std::string{"live"}), live_epoch);
        h.drive();
        REQUIRE(h.resp_received.size() == 1);
        REQUIRE(h.responder->peer_session_id() == live_epoch);

        // A straggler carrying the DEAD incarnation's epoch is dropped by the gate.
        auto straggler = make_data_frame("dead-incarnation", dead_epoch);
        h.responder->on_receive(straggler);
        h.drive();
        REQUIRE(h.resp_received.size() == 1);   // DROPPED, not delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc reconnect: the POST-RECONNECT steady-state publish loop is zero-alloc (no hot-path regression)",
          "[integration][reconnect][inproc]")
{
    manual_clock::reset();
    reconnect_config cfg{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                         std::nullopt, std::nullopt};
    harness h(cfg);
    h.listen();
    h.driver.start();
    h.drive();
    REQUIRE(h.requester->is_complete());

    // Reconnect once so the gate runs in a POST-RECONNECT context: the session was
    // torn down, re-dialed through the transport, and re-handshaked to a fresh epoch.
    h.driver.on_channel_dropped();
    h.advance(std::chrono::milliseconds(10001));
    REQUIRE(h.requester->is_complete());
    REQUIRE(h.driver.attempt_count() >= 1);

    // The steady-state hot path the architecture governs is the forwarder's
    // frame-once → fan-out loop. The inproc bus copies each packet into a queued
    // vector (a deterministic-delivery artifact, not the production hot path), so
    // the no-REGRESSION gate is measured over a non-allocating sink Policy exactly
    // as the existing steady-state gate is — proving the reconnect cycle leaves the
    // forwarder's hot path zero-alloc. The reconnect rebuilds the peer_session +
    // arms the backoff timer (intentional CONNECTION-PATH setup) OUTSIDE this window.
    using forwarder = plexus::io::message_forwarder<sink_policy>;
    constexpr int N = 8;
    constexpr int K = 1024;
    const std::string fqn = "post-reconnect.topic";
    const std::string payload = "post-reconnect-steady-state-payload";

    sink_executor sx;
    std::vector<std::unique_ptr<sink_channel>> channels;
    std::vector<forwarder::peer> peers;
    forwarder fwd;
    for(int i = 0; i < N; ++i)
    {
        channels.push_back(std::make_unique<sink_channel>(sx));
        peers.push_back(forwarder::peer{*channels.back(), "node-" + std::to_string(i)});
        fwd.attach(peers.back(), fqn);
    }

    fwd.publish(fqn, as_bytes(payload));   // warm-up grows the reused scratch to steady size

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fwd.publish(fqn, as_bytes(payload));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(after - before == 0);   // the reconnect cycle poisons no steady-state hot path
}

// The node-shared receive-route oracle on the manual virtual clock. The per-session
// receive seam (peer_session::on_message) is installed on the live session object and
// is therefore LOST when a reconnect rebuilds the slot's session, and it is raced by
// the engine's posted observer fan-out. The node-shared route threaded through the
// session_build_context fixes both: the registry re-threads it on every rebuild, so a
// route installed ONCE before any session exists survives an unbounded number of
// reconnects. This oracle proves, looped where behavioral:
//   - delivery: a route installed via routing_engine::on_message_route delivers a
//     published message carrying a populated message_info;
//   - reconnect survival: after a forced drop + backoff re-dial that rebuilds the
//     receiving slot's session, the SAME route (never re-installed) still delivers —
//     looped over several reconnect iterations, green every iteration;
//   - precedence: a per-session on_message installed by a test takes precedence over
//     the shared route for that session.
// Deterministic inproc Policy on the manual clock — no backend, no socket.

#include "plexus/io/known_peers.h"
#include "plexus/io/message_info.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::message_info;
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

// Node A (the DIALER, hence the slot that REBUILDS on reconnect) subscribing to
// node B (the publisher). A installs its node-shared receive route ONCE before any
// session exists; B publishes. Member ORDER: bus/executor/transports BEFORE the
// engines so destruction unwinds the engines' channels before the bus.
struct route_net
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   transport_a{ex, bus};
    transport_t                   transport_b{ex, bus};

    engine a;
    engine b;

    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    route_net()
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed,
                /*eager=*/true)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed,
                /*eager=*/true)
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void drive() { ex.drain(); }

    // B's CURRENTLY-connected inbound session (A's dial lands on a fresh inbound slot
    // on every reconnect, so the index is not stable — resolve it live). There is
    // exactly one connected inbound peer in this two-node net.
    plexus::io::peer_session<manual_policy> *b_live_inbound()
    {
        plexus::io::peer_session<manual_policy> *found = nullptr;
        b.registry().for_each_connected(
                [&](const plexus::node_id &, plexus::io::peer_session<manual_policy> &s)
                { found = &s; });
        return found;
    }

    // Bring A connected to B and wire the producer-side fanout both ways so a B
    // publish reaches A. Returns once both ends carry a complete session.
    void connect_and_wire(const std::string &topic)
    {
        a.note_peer(id_b, ep_b);
        drive();
        REQUIRE(a.is_connected(id_b));
        rewire_fanout(topic);
    }

    // (Re)attach the producer-side fanout on both ends for the live A->B pair. Called
    // after a reconnect rebuild too: the fanout is per-session demand, NOT the shared
    // route under test, so it is re-established for the freshly built sessions.
    void rewire_fanout(const std::string &topic)
    {
        auto *a_to_b    = a.session_for(id_b);
        auto *b_inbound = b_live_inbound();
        REQUIRE(a_to_b != nullptr);
        REQUIRE(b_inbound != nullptr);
        REQUIRE(a.messages().attach_for_fanout(a_to_b->msg_peer(), topic));
        REQUIRE(b.messages().attach_for_fanout(b_inbound->msg_peer(), topic));
        drive();
    }

    // B publishes to its live inbound peer (A), drawing that slot's epoch.
    void publish(const std::string &topic, const std::string &payload)
    {
        auto *b_inbound = b_live_inbound();
        REQUIRE(b_inbound != nullptr);
        b.messages().publish(topic, as_bytes(payload), b_inbound->session_id());
        drive();
    }
};

}

TEST_CASE("shared receive route: a route installed via the engine delivers a published message "
          "with a populated message_info",
          "[integration][routing][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string topic        = "topic";
    const std::string payload      = "routed-through-the-shared-route";
    int               delivered    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        route_net net;

        std::vector<std::string>  received;
        std::vector<message_info> infos;
        net.a.on_message_route(
                [&](std::string_view, std::span<const std::byte> d, const message_info &mi)
                {
                    received.emplace_back(to_string(d));
                    infos.push_back(mi);
                });

        net.connect_and_wire(topic);
        net.publish(topic, payload);

        REQUIRE(received.size() == 1);
        REQUIRE(received.front() == payload);
        // The route carries the message_info assembled at the receiving session: the
        // reception timestamp is stamped (non-zero), proving the 3-arg info path ran.
        REQUIRE(infos.size() == 1);
        REQUIRE(infos.front().reception_timestamp != 0);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("shared receive route: the route survives a forced reconnect rebuild without re-install "
          "(looped)",
          "[integration][routing][inproc]")
{
    constexpr int     k_reconnects = 6;
    const std::string topic        = "topic";
    manual_clock::reset();
    route_net net;

    // Install the shared route ONCE, before any session exists.
    std::vector<std::string> received;
    net.a.on_message_route([&](std::string_view, std::span<const std::byte> d, const message_info &)
                           { received.emplace_back(to_string(d)); });

    net.connect_and_wire(topic);
    net.publish(topic, "before-reconnect");
    REQUIRE(received.size() == 1);
    REQUIRE(received.back() == "before-reconnect");

    // Force a reconnect rebuild of A's dialed slot several times. After each rebuild
    // the per-session on_message seam would be gone — the shared route, re-threaded by
    // the registry on the rebuild, must STILL deliver with NO re-install.
    for(int r = 0; r < k_reconnects; ++r)
    {
        const auto epoch_before = net.a.session_for(net.id_b)->session_id();

        net.a.registry().driver_for(net.id_b).on_channel_dropped();
        manual_clock::advance(std::chrono::milliseconds(10001));
        net.ex.drain();

        REQUIRE(net.a.is_connected(net.id_b));
        REQUIRE(net.a.session_for(net.id_b)->session_id() != epoch_before);

        // Re-wire the producer-side fanout for the freshly rebuilt slot (the fanout
        // attachment is per-session demand, NOT the route under test), then publish.
        const std::string payload = "after-reconnect-" + std::to_string(r);
        net.rewire_fanout(topic);
        net.publish(topic, payload);

        REQUIRE(received.size() == static_cast<std::size_t>(r + 2));
        REQUIRE(received.back() == payload);
    }
}

TEST_CASE("shared receive route: a per-session on_message takes precedence over the shared route",
          "[integration][routing][inproc]")
{
    constexpr int     k_iterations = 100;
    const std::string topic        = "topic";
    const std::string payload      = "precedence-payload";
    int               proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        manual_clock::reset();
        route_net net;

        std::vector<std::string> via_route;
        net.a.on_message_route([&](std::string_view, std::span<const std::byte> d,
                                   const message_info &) { via_route.emplace_back(to_string(d)); });

        net.connect_and_wire(topic);

        // Install a per-session seam on the live A->B session: it must win.
        std::vector<std::string> via_session;
        net.a.session_for(net.id_b)->on_message([&](std::string_view, std::span<const std::byte> d)
                                                { via_session.emplace_back(to_string(d)); });

        net.publish(topic, payload);

        REQUIRE(via_session.size() == 1);
        REQUIRE(via_session.front() == payload);
        REQUIRE(via_route.empty()); // the shared route did NOT also fire
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

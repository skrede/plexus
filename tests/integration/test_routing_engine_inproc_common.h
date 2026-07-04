#ifndef HPP_GUARD_TESTS_INTEGRATION_ROUTING_ENGINE_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_ROUTING_ENGINE_INPROC_COMMON_H

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
// start() tail. This is the deterministic routing oracle the engine headers satisfy.

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

namespace routing_inproc_fixture {

// The virtual clock the handshake + backoff timers fire from, and the matching
// Policy — identical in shape to the reconnect oracle's manual_clock.
struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    [[maybe_unused]] static constexpr bool is_steady = false;

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

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
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
    plexus::log::null_logger sink;

    engine a;
    engine b;

    discovery_stub discovery;
    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint ep_a{"inproc", "node-a"};
    endpoint ep_b{"inproc", "node-b"};

    explicit two_node(bool eager = false)
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, eager)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, eager)
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

#endif

#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_GRAPH_CHANGE_INPROC_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_GRAPH_CHANGE_INPROC_H

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/graph/graph_change.h"
#include "plexus/graph/topic_record.h"

#include "plexus/io/observer.h"

#include "plexus/log/logger.h"

#include "plexus/policy.h"
#include "plexus/node_id.h"

#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

// The shared inproc harness the graph-change observer suites drive against. It mirrors the
// recording-observer-on-a-real-engine + manual virtual clock + drain-then-assert oracle in
// tests/integration/test_peer_observer_inproc.cpp, narrowed to the graph mutation seams. These TUs are
// authored against the FINAL intended API — plexus::io::observer::on_graph_changed/on_graph_delta/
// observes_graph, plexus::graph::graph_change, and routing_engine::graph_generation — and are RED
// until the implementing waves land the symbols.

namespace plexus::testing::graph_change_fixture {

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
    using executor_type     = plexus::inproc::inproc_executor<manual_clock> &;
    using byte_channel_type = plexus::inproc::inproc_channel<manual_clock>;
    using timer_type        = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = plexus::inproc::inproc_transport<manual_clock>;
using engine      = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

// The bounded-profile engine: the fixed-capacity peer + topic twins the MCU path selects. It joins
// the coarse graph path SC#4 proves allocation-free; the host edge-log is structurally absent here.
using bounded_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock,
                                                  plexus::io::fixed_peer_storage<8>, plexus::graph::fixed_topic_storage<8>>;

constexpr auto k_long_timeout  = std::chrono::hours(1);
constexpr auto k_ceiling       = std::chrono::milliseconds(10001);
constexpr std::uint64_t k_seed = 0xC0FFEEu;

inline plexus::io::handshake_fsm_config make_cfg(std::uint8_t id_seed, std::uint8_t version = 1, std::uint8_t compatible = 1)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return plexus::io::handshake_fsm_config{.self_id = id, .version_major = version, .version_minor = 0, .compatible_version_major = compatible, .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_id inbound_slot(std::uint8_t n)
{
    plexus::node_id id = make_id(0x00);
    id[15]             = std::byte{n};
    return id;
}

inline plexus::io::endpoint ep_for(const char *address)
{
    return plexus::io::endpoint{"inproc", address};
}

inline plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

// Counts each graph EDGE the engine posts. The coarse counter and the last generation prove the
// single-wakeup coalescing (SC#1) + the D-09 generation argument; the deltas vector captures the
// GRAPH-07 host payload; graph_opt_in drives the observes_graph opt-in gate.
struct recording_graph_observer final : public plexus::io::observer
{
    int changed_fires{0};
    std::uint64_t last_generation{0};
    std::vector<plexus::graph::graph_change> deltas;
    bool graph_opt_in{false};

    void on_graph_changed(std::uint64_t generation) override
    {
        ++changed_fires;
        last_generation = generation;
    }

    void on_graph_delta(const plexus::graph::graph_change &change) override
    {
        deltas.push_back(change);
    }

    bool observes_graph() const override
    {
        return graph_opt_in;
    }
};

// A single engine over the inproc backend — the graph mutation seams (note_peer/forget,
// note_local_topic/forget_local_topic) are driven directly and every assertion is made after drain().
template<typename Engine>
struct basic_one_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t transport{ex, bus};
    plexus::log::null_logger sink;
    Engine eng;
    plexus::node_id self{make_id(0xA1)};

    basic_one_node()
            : eng(transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        eng.listen(ep_for("node-a"));
    }

    void drain()
    {
        ex.drain();
    }
};

using one_node         = basic_one_node<engine>;
using bounded_one_node = basic_one_node<bounded_engine>;

// A two-node rendezvous on one bus, mirroring the peer-observer oracle: A dials B so the redial +
// mid-flight teardown legs (SC#3) have a real session to tear down.
struct two_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};
    plexus::log::null_logger sink;

    engine a;
    engine b;

    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    plexus::io::endpoint ep_a{ep_for("node-a")};
    plexus::io::endpoint ep_b{ep_for("node-b")};

    two_node()
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false)
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

#endif

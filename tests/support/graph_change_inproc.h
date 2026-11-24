#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_GRAPH_CHANGE_INPROC_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_GRAPH_CHANGE_INPROC_H

#include "graph_change_clock.h"

#include "plexus/io/observer.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_options.h"
#include "plexus/io/fixed_peer_storage.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_session_registry.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/graph/graph_change.h"
#include "plexus/graph/topic_record.h"
#include "plexus/graph/fixed_topic_storage.h"

#include "plexus/log/logger.h"

#include "plexus/policy.h"
#include "plexus/node_id.h"
#include "plexus/executor_verbs.h"

#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::testing::graph_change_fixture {

using transport_t = plexus::inproc::inproc_transport<manual_clock>;
using engine      = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

// The fixed-capacity peer and topic storage the MCU profile selects, which carries no host edge-log.
using bounded_engine = plexus::io::routing_engine<manual_policy, transport_t, manual_clock, plexus::io::fixed_peer_storage<8>, plexus::graph::fixed_topic_storage<8>>;

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

inline plexus::io::endpoint ep_for(const char *address)
{
    return plexus::io::endpoint{"inproc", address};
}

inline plexus::io::reconnect_options forever_cfg()
{
    return plexus::io::reconnect_options{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

// Counts each graph edge the engine posts, so mutations coalesced into one wakeup count once.
struct recording_graph_observer final : public plexus::io::observer
{
    int changed_fires;
    bool graph_opt_in;
    std::uint64_t last_generation;
    std::vector<plexus::graph::graph_change> deltas;

    recording_graph_observer()
            : changed_fires(0)
            , graph_opt_in(false)
            , last_generation(0)
    {
    }

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

// The graph mutation seams are driven directly, and every assertion is made after drain().
template<typename Engine>
struct basic_one_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex;
    transport_t transport;
    plexus::log::null_logger sink;
    Engine eng;
    plexus::node_id self;

    explicit basic_one_node(const plexus::io::liveliness_options &live = {})
            : ex{bus}
            , transport{ex, bus}
            , eng(transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, live)
            , self{make_id(0xA1)}
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

// Two nodes on one bus, so the redial and mid-flight teardown legs have a real session to tear down.
struct two_node
{
    plexus::inproc::inproc_bus<manual_clock> bus;
    plexus::inproc::inproc_executor<manual_clock> ex;
    transport_t transport_a;
    transport_t transport_b;
    plexus::log::null_logger sink;

    engine a;
    engine b;

    plexus::node_id id_a;
    plexus::node_id id_b;
    plexus::io::endpoint ep_a;
    plexus::io::endpoint ep_b;

    two_node()
            : ex{bus}
            , transport_a{ex, bus}
            , transport_b{ex, bus}
            , a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , id_a{make_id(0xA1)}
            , id_b{make_id(0xB2)}
            , ep_a{ep_for("node-a")}
            , ep_b{ep_for("node-b")}
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

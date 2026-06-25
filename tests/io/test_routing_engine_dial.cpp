// The dial-by-endpoint oracle for the general point-to-point verb. endpoint_id is a pure digest
// proven for determinism and distinctness; the engine dial path is proven over a two-engine inproc
// link sharing one bus — A dials the endpoint B listens on, the handshake completes both ends, and a
// second dial of the same endpoint mints no second slot (the hash-of-endpoint redial-dedup contract).

#include "plexus/io/endpoint_id.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>

using plexus::io::endpoint;
using plexus::io::endpoint_id;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_transport;
using engine = plexus::io::routing_engine<inproc_policy, inproc_transport<>>;

namespace {

constexpr auto k_long_timeout  = std::chrono::hours(1);
constexpr std::uint64_t k_seed = 0xD1A1u;

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(5), std::chrono::milliseconds(200), std::nullopt, std::nullopt};
}

// Two engines on ONE shared inproc bus/executor: B listens, A dials B's endpoint. Member ORDER:
// bus/executor/transports BEFORE the engines so destruction unwinds each engine's sessions/channels
// before the bus they register on.
struct dial_pair
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> a_transport{ex, bus};
    inproc_transport<> b_transport{ex, bus};
    plexus::log::null_logger sink;
    engine a{a_transport, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink};
    engine b{b_transport, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed + 1, sink};

    dial_pair()
    {
        b.listen({"inproc", "svc"});
    }

    int connected_count()
    {
        int n = 0;
        a.registry().for_each_connected([&](const plexus::node_id &, auto &) { ++n; });
        return n;
    }
};

int count_distinct(std::initializer_list<plexus::node_id> ids)
{
    int distinct = 0;
    for(auto outer = ids.begin(); outer != ids.end(); ++outer)
    {
        bool seen = false;
        for(auto inner = ids.begin(); inner != outer; ++inner)
            seen = seen || (*inner == *outer);
        distinct += seen ? 0 : 1;
    }
    return distinct;
}

}

TEST_CASE("routing_engine_dial: endpoint_id is deterministic and endpoint-distinct", "[io][dial]")
{
    const endpoint serial{"serial", "/dev/ttyUSB0@115200"};
    REQUIRE(endpoint_id(serial) == endpoint_id(serial));

    // The separator byte keeps a scheme/address split ambiguity from colliding.
    REQUIRE(endpoint_id({"se", "rial-x"}) != endpoint_id({"ser", "ial-x"}));
    REQUIRE(count_distinct({endpoint_id({"serial", "/dev/ttyUSB0"}), endpoint_id({"serial", "/dev/ttyUSB1"}), endpoint_id({"inproc", "/dev/ttyUSB0"})}) == 3);
}

TEST_CASE("routing_engine_dial: dialing an endpoint builds exactly one completed slot", "[io][dial]")
{
    dial_pair p;
    const endpoint ep{"inproc", "svc"};

    p.a.dial(ep);
    p.ex.drain();

    REQUIRE(p.a.is_connected(endpoint_id(ep)));
    REQUIRE(p.a.session_for(endpoint_id(ep))->session_id() != 0);
    REQUIRE(p.connected_count() == 1);
}

TEST_CASE("routing_engine_dial: redialing the same endpoint mints no second slot", "[io][dial]")
{
    dial_pair p;
    const endpoint ep{"inproc", "svc"};

    p.a.dial(ep);
    p.ex.drain();
    REQUIRE(p.a.is_connected(endpoint_id(ep)));
    const auto epoch = p.a.session_for(endpoint_id(ep))->session_id();

    // The same endpoint hashes to the same provisional id, so ensure_slot's find early-returns and
    // endpoint_claimed never trips: the persistent slot and its epoch survive the redial.
    p.a.dial(ep);
    p.ex.drain();

    REQUIRE(p.connected_count() == 1);
    REQUIRE(p.a.session_for(endpoint_id(ep))->session_id() == epoch);
}

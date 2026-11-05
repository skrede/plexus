#ifndef HPP_GUARD_TESTS_INTEGRATION_REACH_GUARD_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_REACH_GUARD_COMMON_H

#include "test_routing_engine_inproc_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/node_id.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace reach_guard_fixture {

using namespace routing_inproc_fixture;

constexpr auto k_ceiling = std::chrono::milliseconds(10001);
constexpr auto k_tick    = std::chrono::milliseconds(100);

inline reconnect_config bounded_cfg(std::optional<std::uint32_t> attempts, std::optional<std::chrono::milliseconds> elapsed)
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), attempts, elapsed};
}

struct guarded_pair
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t transport_a{ex, bus};
    transport_t transport_b{ex, bus};
    plexus::log::null_logger sink;
    engine a;
    engine b;
    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint ep_a{"inproc", "node-a"};
    endpoint ep_b{"inproc", "node-b"};

    explicit guarded_pair(const reconnect_config &redial = forever_cfg(), bool eager = false)
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, redial, k_seed, sink, eager)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, eager)
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }

    template<typename Done>
    void advance_until(Done done)
    {
        for(std::int32_t i = 0; i < 10 && !done(); ++i)
            advance(k_ceiling);
    }

    engine::session_type *step_into_handshake(const plexus::node_id &key)
    {
        while(a.session_for(key) == nullptr && ex.step())
        {
        }
        return a.session_for(key);
    }

    std::size_t complete_sessions_for_b()
    {
        std::size_t n = 0;
        a.registry().for_each_connected([&](const plexus::node_id &, engine::session_type &s) { n += s.peer_identity() == id_b ? 1 : 0; });
        return n;
    }
};

}

#endif

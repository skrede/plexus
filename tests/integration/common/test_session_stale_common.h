#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_STALE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_STALE_COMMON_H

#include "test_session_stale_tap.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/node_id.h"

#include <chrono>

namespace session_stale_fixture {

using plexus::io::k_tick_granularity;

using stale_engine = plexus::io::routing_engine<manual_policy, tapping_transport, manual_clock>;

inline std::chrono::nanoseconds miss_window(const plexus::io::liveliness_options &live = {})
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(live.heartbeat_interval) * live.heartbeat_miss_limit;
}

inline plexus::io::liveliness_options with_policy(plexus::io::combine policy)
{
    plexus::io::liveliness_options live{};
    live.policy = policy;
    return live;
}

inline plexus::io::liveliness_options with_teardown(std::chrono::nanoseconds timeout, plexus::io::liveliness_options live = {})
{
    live.session_teardown_timeout = timeout;
    return live;
}

inline plexus::io::liveliness_options teardown_at_miss_window(const plexus::io::liveliness_options &live = {})
{
    return with_teardown(miss_window(live), live);
}

struct stale_pair
{
    explicit stale_pair(const plexus::io::liveliness_options &live = {})
            : bus()
            , ex(bus)
            , transport_a(ex, bus)
            , transport_b(ex, bus)
            , sink()
            , a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, live)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, live)
            , id_a(make_id(0xA1))
            , id_b(make_id(0xB2))
            , ep_a("inproc", "node-a")
            , ep_b("inproc", "node-b")
    {
        a.listen(ep_a);
        b.listen(ep_b);
    }

    void connect()
    {
        a.note_peer(id_b, ep_b);
        a.reach(id_b);
        ex.drain();
    }

    void advance(std::chrono::nanoseconds d)
    {
        manual_clock::advance(d);
        ex.drain();
    }

    template<typename Done>
    std::chrono::nanoseconds tick_until(Done done, std::chrono::nanoseconds limit)
    {
        std::chrono::nanoseconds elapsed{0};
        while(!done() && elapsed < limit)
        {
            advance(k_tick_granularity);
            elapsed += k_tick_granularity;
        }
        return elapsed;
    }

    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    tapping_transport transport_a;
    tapping_transport transport_b;
    plexus::log::null_logger sink;
    stale_engine a;
    stale_engine b;
    plexus::node_id id_a;
    plexus::node_id id_b;
    endpoint ep_a;
    endpoint ep_b;
};

}

#endif

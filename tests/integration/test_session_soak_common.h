#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_SOAK_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_SOAK_COMMON_H

#include "test_routing_engine_inproc_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/disconnect_cause.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/liveliness_options.h"

#include "plexus/io/detail/peer_session_lifecycle.h"

#include "plexus/node_id.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

namespace session_soak_fixture {

using namespace routing_inproc_fixture;
using plexus::io::k_tick_granularity;

constexpr std::string_view k_topic{"/soak/topic"};
constexpr auto k_settle_limit = std::chrono::seconds(30);

// A session held past this teardown survives only on heartbeats: nothing else crosses the wire once the subscribe has settled.
inline plexus::io::liveliness_options soak_liveliness()
{
    plexus::io::liveliness_options live{};
    live.session_teardown_timeout = std::chrono::duration_cast<std::chrono::nanoseconds>(live.heartbeat_interval) * live.heartbeat_miss_limit;
    return live;
}

struct edge_tally final : plexus::io::observer
{
    edge_tally()
            : connected(0)
            , disconnected(0)
            , silenced(0)
    {
    }

    void on_peer_connected(const plexus::node_id &, std::string_view, plexus::io::peer_kind) override
    {
        ++connected;
    }

    void on_peer_disconnected_with_cause(const plexus::node_id &, std::string_view, plexus::io::peer_kind, plexus::io::disconnect_cause cause) override
    {
        ++disconnected;
        silenced += cause == plexus::io::disconnect_cause::heartbeat_timeout ? 1 : 0;
    }

    std::int32_t connected;
    std::int32_t disconnected;
    std::int32_t silenced;
};

// a dials b by its card, so a keeps one dialed slot across every cycle and each session b holds for a is an accepted one.
struct soak_net
{
    soak_net()
            : id_a(make_id(0xA1))
            , id_b(make_id(0xB2))
            , ep_a("inproc", "soak-a")
            , ep_b("inproc", "soak-b")
            , bus()
            , ex(bus)
            , transport_a(ex, bus)
            , transport_b(ex, bus)
            , sink()
            , seen()
            , a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, soak_liveliness())
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, soak_liveliness())
    {
        a.listen(ep_a);
        b.listen(ep_b);
        b.add_observer(seen);
        a.messages().declare(k_topic, plexus::topic_qos{}, std::nullopt, true);
    }

    template<typename Done>
    bool step_until(Done done)
    {
        for(std::chrono::nanoseconds t{0}; !done() && t < k_settle_limit; t += k_tick_granularity)
        {
            manual_clock::advance(k_tick_granularity);
            ex.drain();
        }
        return done();
    }

    bool connected() const
    {
        return a.is_connected(id_b) && b.is_connected(id_a);
    }

    void connect()
    {
        a.note_peer(id_b, ep_b);
        a.reach(id_b);
        ex.drain();
        REQUIRE(step_until([this] { return connected(); }));

        b.subscribe(id_a, k_topic);
        ex.drain();
    }

    void hold()
    {
        const plexus::io::liveliness_options live = soak_liveliness();
        const auto until                          = manual_clock::now() + live.session_teardown_timeout + k_tick_granularity;
        step_until([until] { return manual_clock::now() >= until; });

        REQUIRE(connected());
        REQUIRE(!plexus::io::detail::session_silent(*a.session_for(id_b), live.heartbeat_interval));
        REQUIRE(!plexus::io::detail::session_silent(*accepted_from_a(), live.heartbeat_interval));
    }

    engine::session_type *accepted_from_a()
    {
        engine::session_type *found = nullptr;
        b.registry().for_each_connected(
                [&](const plexus::node_id &, engine::session_type &s)
                {
                    if(s.peer_identity() == id_a)
                        found = &s;
                });
        REQUIRE(found != nullptr);
        return found;
    }

    plexus::node_id id_a;
    plexus::node_id id_b;
    endpoint ep_a;
    endpoint ep_b;
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    transport_t transport_a;
    transport_t transport_b;
    plexus::log::null_logger sink;
    edge_tally seen;
    engine a;
    engine b;
};

}

#endif

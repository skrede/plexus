#ifndef HPP_GUARD_TESTS_INTEGRATION_OBS_TAP_POINTS_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_OBS_TAP_POINTS_COMMON_H

// The deterministic data-path tap-point oracle on the manual virtual clock. A recording
// observer is registered on a publishing routing_engine and the message-fan-out taps are
// forced: a single publish to a topic with N attached subscribers fires on_message_published
// once and on_message_delivered once per destination, both POSTED (counters are zero at the
// publish call return, nonzero only after the executor is pumped), and the delivered view
// BORROWS the surfaced buffer's owner (a shared addref, not a fresh copy). Mirrors the
// peer-observer oracle's two-node inproc shape, widened to N subscriber nodes on one bus.

#include "recording_observer.h"

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace obs_tap_fixture {

struct manual_clock
{
    using duration                  = std::chrono::nanoseconds;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept
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

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
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

// One publisher and N subscriber engines on a single inproc bus and executor: every
// subscriber demands "topic" from the publisher, so a single publish fans to N destinations
// through the production attach path (no faked attach).
struct fan_net
{
    static constexpr std::uint8_t k_pub_seed = 0xD0;
    static constexpr const char  *k_topic    = "topic";

    explicit fan_net(std::size_t subscriber_count)
            : pub_transport(ex, bus)
            , pub(pub_transport, ex, make_cfg(k_pub_seed), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        pub.listen({"inproc", "pub"});
        for(std::size_t i = 0; i < subscriber_count; ++i)
        {
            const auto        seed = static_cast<std::uint8_t>(0x10 + i);
            auto              t    = std::make_unique<transport_t>(ex, bus);
            auto              e    = std::make_unique<engine>(*t, ex, make_cfg(seed), k_long_timeout, forever_cfg(), k_seed, sink, false);
            const std::string name = "sub-" + std::to_string(i);
            e->listen({"inproc", name});
            e->note_peer(pub_id(), {"inproc", "pub"});
            e->subscribe(pub_id(), k_topic);
            sub_transports.push_back(std::move(t));
            subs.push_back(std::move(e));
        }
    }

    static plexus::node_id pub_id()
    {
        return make_id(k_pub_seed);
    }

    void drive()
    {
        ex.drain();
    }

    inproc_bus<manual_clock>                  bus;
    inproc_executor<manual_clock>             ex{bus};
    transport_t                               pub_transport;
    plexus::log::null_logger                  sink;
    engine                                    pub;
    std::vector<std::unique_ptr<transport_t>> sub_transports;
    std::vector<std::unique_ptr<engine>>      subs;
};

// A caller and a provider engine on one inproc bus, connected through the production
// dial+handshake path so the RPC taps fire over a real session. Caller fires on_rpc_call +
// on_rpc_reply; provider fires on_rpc_serve. The observer is registered on BOTH.
struct rpc_net
{
    static constexpr std::uint8_t k_caller_seed   = 0xA1;
    static constexpr std::uint8_t k_provider_seed = 0xB2;
    static constexpr const char  *k_proc          = "svc";

    rpc_net()
            : caller_transport(ex, bus)
            , provider_transport(ex, bus)
            , caller(caller_transport, ex, make_cfg(k_caller_seed), k_long_timeout, forever_cfg(), k_seed, sink, false)
            , provider(provider_transport, ex, make_cfg(k_provider_seed), k_long_timeout, forever_cfg(), k_seed, sink, false)
    {
        caller.listen({"inproc", "caller"});
        provider.listen({"inproc", "provider"});
        caller.note_peer(provider_id(), {"inproc", "provider"});
        caller.reach(provider_id());
    }

    static plexus::node_id caller_id()
    {
        return make_id(k_caller_seed);
    }
    static plexus::node_id provider_id()
    {
        return make_id(k_provider_seed);
    }

    void drive()
    {
        ex.drain();
    }

    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   caller_transport;
    transport_t                   provider_transport;
    plexus::log::null_logger      sink;
    engine                        caller;
    engine                        provider;
};

}

#endif

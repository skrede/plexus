#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_LIVELINESS_TRANSPORT_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_LIVELINESS_TRANSPORT_COMMON_H

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/observer.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/pump.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace liveliness_transport_fixture {

using namespace std::chrono_literals;
using plexus::io::endpoint;
using plexus::io::liveliness_verdict;
using clock_type = std::chrono::steady_clock;

struct stamped_verdict
{
    plexus::io::peer_liveliness_event event;
    clock_type::time_point at;
};

struct verdict_recorder final : plexus::io::observer
{
    std::vector<stamped_verdict> verdicts;

    void on_peer_liveliness(const plexus::io::peer_liveliness_event &ev) override
    {
        verdicts.push_back(stamped_verdict{ev, clock_type::now()});
    }

    bool observes_liveliness() const override
    {
        return true;
    }
};

inline plexus::node_options pair_options(std::uint64_t seed, plexus::io::combine policy)
{
    plexus::node_options opts;
    // The default policy reads a silent peer lost only once its session is torn down, so the teardown follows the miss window for lost_bound().
    opts.liveliness.policy                   = policy;
    opts.liveliness.session_teardown_timeout = opts.liveliness.heartbeat_interval * opts.liveliness.heartbeat_miss_limit;
    opts.reconnect                           = plexus::io::reconnect_config{20ms, 40ms, std::nullopt, std::nullopt};
    opts.redial_seed                         = seed;
    return opts;
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline std::chrono::milliseconds lost_bound()
{
    const plexus::io::liveliness_options live{};
    return live.heartbeat_interval * live.heartbeat_miss_limit + 1s;
}

// Node "a" carries the recorder and dials node "b" directly, so neither awareness nor a discovery-driven dial can
// assert the peer alive before the session does.
template<typename Policy, typename Transport>
class node_pair
{
    using node_type = plexus::node<Policy, Transport>;

    struct listener
    {
        listener(::asio::io_context &io, plexus::discovery::static_discovery &disc, plexus::io::combine policy)
                : transport(io)
                , node(io, disc, make_id(0x0B), transport, pair_options(0xBEEFu, policy))
        {
        }

        Transport transport;
        node_type node;
    };

public:
    explicit node_pair(std::string_view scheme, plexus::io::combine policy = plexus::io::combine::any_signal_alive)
            : m_policy(policy)
            , m_io()
            , m_recorder()
            , m_disc_a({})
            , m_disc_b({})
            , m_ta(m_io)
            , m_a(m_io, m_disc_a, make_id(0x0A), m_ta, pair_options(0xC0FFEEu, policy))
            , m_b(std::in_place, m_io, m_disc_b, policy)
            , m_ep_b{std::string{scheme}, ""}
    {
        m_a.router().add_observer(m_recorder);
        m_b->node.listen({std::string{scheme}, "127.0.0.1:0"});
        pump_until([this] { return m_b->transport.port() != 0; }, 2s);
        m_ep_b.address = "127.0.0.1:" + std::to_string(m_b->transport.port());
    }

    clock_type::time_point dial()
    {
        const clock_type::time_point t0 = clock_type::now();
        m_a.dial(m_ep_b);
        return t0;
    }

    void close_b()
    {
        m_b->node.close();
    }

    // The new b listens on the endpoint a dialed, so a's redial reaches it on the same port.
    bool restart_b()
    {
        bool closed = false;
        m_b->node.on_closed([&closed] { closed = true; });
        m_b->node.close();
        if(!pump_until([&closed] { return closed; }, 2s))
            return false;

        m_b.emplace(m_io, m_disc_b, m_policy);
        m_b->node.listen(m_ep_b);
        return pump_until([this] { return m_b->transport.port() != 0; }, 2s) && m_ep_b.address == "127.0.0.1:" + std::to_string(m_b->transport.port());
    }

    template<typename Pred>
    bool pump_until(Pred pred, std::chrono::milliseconds bound)
    {
        return plexus::testing::pump_until(m_io, std::move(pred), bound);
    }

    std::optional<std::chrono::milliseconds> first_alive_after(clock_type::time_point t0) const
    {
        const auto it = std::ranges::find_if(m_recorder.verdicts, [&](const stamped_verdict &v) { return is_b(v, liveliness_verdict::alive) && v.at >= t0; });
        if(it == m_recorder.verdicts.end())
            return std::nullopt;
        return std::chrono::duration_cast<std::chrono::milliseconds>(it->at - t0);
    }

    bool lost_seen() const
    {
        return std::ranges::any_of(m_recorder.verdicts, [this](const stamped_verdict &v) { return is_b(v, liveliness_verdict::lost); });
    }

private:
    plexus::io::combine m_policy;
    ::asio::io_context m_io;
    verdict_recorder m_recorder;
    plexus::discovery::static_discovery m_disc_a;
    plexus::discovery::static_discovery m_disc_b;
    Transport m_ta;
    node_type m_a;
    std::optional<listener> m_b;
    endpoint m_ep_b;

    bool is_b(const stamped_verdict &v, liveliness_verdict verdict) const
    {
        return v.event.verdict == verdict && v.event.id == plexus::io::endpoint_id(m_ep_b);
    }
};

}

#endif

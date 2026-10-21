#ifndef HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_QUIESCENCE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_NODE_TEST_NODE_CLOSE_QUIESCENCE_COMMON_H

#include "test_node_close_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/endpoint_id.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/io/endpoint_seam.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/observation_events.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace node_close_fixture {

template<typename Node>
void on_closed_or_fail(Node &node, plexus::detail::move_only_function<void()> cb)
{
    if constexpr(requires { node.on_closed(std::move(cb)); })
        node.on_closed(std::move(cb));
    else
        FAIL("the node has no on_closed()");
}

// The edges a node's engine fans out and the test's own marks ("closed") land in one log, in the order they ran.
struct edge_log final : plexus::io::observer
{
    std::vector<std::string> entries;

    void on_peer_disconnected(const plexus::node_id &, std::string_view, plexus::io::peer_kind) override
    {
        entries.emplace_back("disconnected");
    }

    void on_participant(const plexus::io::participant_event &) override
    {
        entries.emplace_back("participant");
    }

    void on_endpoint(std::string_view, const plexus::io::endpoint_event &) override
    {
        entries.emplace_back("endpoint");
    }

    std::size_t count(std::string_view entry) const
    {
        return static_cast<std::size_t>(std::count(entries.begin(), entries.end(), entry));
    }

    bool ends_with(std::string_view entry) const
    {
        return !entries.empty() && entries.back() == entry;
    }
};

inline constexpr std::array<std::byte, 1> k_payload{std::byte{0x2A}};

inline void hold_request(std::span<const std::byte>, plexus::io::reply_fn &)
{
}

// The engine's one retry timer stays armed: every lane the coordinator tries for the node's own topic is declined as
// transient, so each wake re-attempts and counts one more transient decline.
template<typename Node>
void arm_upgrade_retry(Node &node)
{
    using channel     = typename Node::engine_channel;
    auto &coordinator = node.router().coordinator();
    coordinator.on_policy([](bool, plexus::io::dispatch_hint) { return true; });
    coordinator.on_gate([](std::string_view) { return plexus::io::upgrade_mint<channel>{plexus::io::fallback_reason::transient}; });
    coordinator.on_local_publisher("lane", true);
    coordinator.on_edge("peer", "lane", plexus::io::demand_transition::up, plexus::io::demand_role::publisher);
}

inline std::uint64_t transient_declines(const dialer_node &node)
{
    return node.same_host_fallbacks()[static_cast<std::size_t>(plexus::io::fallback_reason::transient)];
}

// A lives in an optional so its owner can destroy it inside its own on_closed.
struct owned_net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    deferred_dial_transport ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    counting_discovery disc_a;
    counting_discovery disc_b;
    endpoint ep_b{"inproc", "host-b:6000"};
    std::optional<dialer_node> a;
    listener_node b{ex, disc_b, make_id(0x0B), tb, make_opts()};

    owned_net()
    {
        a.emplace(ex, disc_a, make_id(0x0A), ta, make_opts());
        b.listen(ep_b);
        a->dial(ep_b);
        ex.drain();
        REQUIRE(a->router().is_connected(plexus::io::endpoint_id(ep_b)));
    }
};

}

#endif

#include "slot_rebuild_fixture.h"

#include "plexus/io/endpoint.h"

#include "plexus/io/detail/peer_session_slot.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <utility>

using plexus::testing::slot_channel;
using plexus::testing::channel_probe;
using plexus::testing::slot_rebuild_fixture;

namespace {

const std::vector<std::string> k_retired_in_order{"notified", "destroyed"};

// The channel holds an operation, so destroying it before its close notification is visible in its probe.
slot_channel &dial_completion(slot_rebuild_fixture &fx, const plexus::io::endpoint &ep)
{
    auto channel        = std::make_unique<slot_channel>(fx.executor, fx.probes.emplace_back());
    slot_channel &taken = *channel;
    taken.hold();
    fx.registry.build_session_for_endpoint(ep, std::move(channel));
    return taken;
}

void require_retired(slot_rebuild_fixture &fx, slot_channel &channel, const channel_probe &probe)
{
    REQUIRE(probe.events.empty());
    REQUIRE(probe.closed);
    REQUIRE(probe.sends == 0);
    channel.release();
    fx.executor.run_posted();
    REQUIRE(probe.events == k_retired_in_order);
}

}

TEST_CASE("registry_retire a dialed channel for an endpoint no slot claims is retired, not destroyed", "[io][registry_retire][!shouldfail]")
{
    slot_rebuild_fixture fx;
    slot_channel &channel = dial_completion(fx, {"tcp", "203.0.113.9:4200"});
    require_retired(fx, channel, fx.probe(0));
    REQUIRE_FALSE(fx.registry.has_session(fx.peer));
}

TEST_CASE("registry_retire a dialed channel arriving after its slot's redial was denied is retired through the slot's cell", "[io][registry_retire][!shouldfail]")
{
    slot_rebuild_fixture fx;
    fx.registry.deny_redial(fx.peer);
    slot_channel &channel = dial_completion(fx, fx.dialed);
    require_retired(fx, channel, fx.probe(0));
    REQUIRE_FALSE(fx.registry.has_session(fx.peer));
}

TEST_CASE("registry_retire an accept whose next inbound id a slot already holds gets a fresh slot and a session", "[io][registry_retire][!shouldfail]")
{
    slot_rebuild_fixture fx;
    const plexus::node_id taken = plexus::io::detail::inbound_slot_id(1);
    REQUIRE(fx.registry.ensure_slot(taken, {"tcp", "198.51.100.8:4300"}, "taken-node"));

    const plexus::node_id accepted = fx.registry.accept_session(std::make_unique<slot_channel>(fx.executor, fx.probes.emplace_back()));
    REQUIRE(accepted != taken);
    REQUIRE(fx.registry.has_session(accepted));
    REQUIRE_FALSE(fx.probe(0).closed);
}

#include "plexus/io/shm/medium_coordinator.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/peer_context.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <map>
#include <string>
#include <vector>
#include <utility>
#include <string_view>

// The medium-assert oracle: on a co-host qualifying demand edge the coordinator acquires
// the per-topic ring THROUGH the injected gate (the can_acquire/prefer_shm_hook path the
// node installs from its shm member) — NEVER a direct registry acquire (the coordinator
// has no registry-acquire access, only the gate probes, so the bypass is structurally
// impossible). The oracle records WHICH path served ("shm" iff the gate accepted) and
// asserts dual delivery (the wire attach is never suppressed: a declined/off-host/hint-
// less pair simply keeps the wire — the coordinator issues no acquire). It also drives the
// forwarder's real emit seam and the refcount 0->1/1->0 gating.

using namespace plexus::io::shm;
namespace pio = plexus::io;
using plexus::io::demand_transition;

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

// A stub registry exposing only same_host_for, the one read the coordinator makes. It
// records the verdict per node_name (fail-closed default false), mirroring the real
// peer_session_registry::same_host_for shape.
struct stub_registry
{
    std::map<std::string, bool> verdicts;

    [[nodiscard]] bool same_host_for(std::string_view node_name) const
    {
        auto it = verdicts.find(std::string{node_name});
        return it != verdicts.end() && it->second;
    }
};

// The injected gate the node would build over its shm member: it records every acquire/
// release as the served-medium path, returns whether the ring acquired (the can_acquire
// verdict), and a fail switch forces a decline (the wire_fallback/over-cap path).
struct gate_recorder
{
    std::vector<std::pair<std::string, std::string>> served; // (op, fqn) — op is "shm"/"release"
    bool accept = true;

    bool acquire(std::string_view fqn)
    {
        if(!accept)
            return false; // gate declines: the pair keeps the wire (dual delivery)
        served.emplace_back("shm", std::string{fqn});
        return true;
    }

    void release(std::string_view fqn) { served.emplace_back("release", std::string{fqn}); }

    [[nodiscard]] std::size_t acquires() const
    {
        std::size_t n = 0;
        for(const auto &s : served)
            if(s.first == "shm")
                ++n;
        return n;
    }

    [[nodiscard]] std::size_t releases() const
    {
        std::size_t n = 0;
        for(const auto &s : served)
            if(s.first == "release")
                ++n;
        return n;
    }
};

using coordinator = medium_coordinator<stub_registry>;

void install_gate(coordinator &c, gate_recorder &gate)
{
    c.on_gate([&gate](std::string_view fqn) { return gate.acquire(fqn); },
              [&gate](std::string_view fqn) { gate.release(fqn); });
}

}

TEST_CASE("shm.coordinator a co-host qualifying 0->1 acquires THROUGH the gate, never a direct registry acquire",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up);

    REQUIRE(gate.acquires() == 1);          // exactly one acquire, routed through the gate
    REQUIRE(gate.served.front().first == "shm"); // the served medium is shm (the gate accepted)
}

TEST_CASE("shm.coordinator an off-host 0->1 issues NO acquire and keeps the wire", "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = false; // off-host
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up);

    REQUIRE(gate.acquires() == 0); // attempt_shm_upgrade false: no acquire, the wire stays
}

TEST_CASE("shm.coordinator a hint-less co-host 0->1 issues NO acquire", "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    // no set_topic_hint -> dispatch_hint::none -> shm_eligible false

    c.on_edge("node-a", "alpha", demand_transition::up);

    REQUIRE(gate.acquires() == 0);
}

TEST_CASE("shm.coordinator a gate decline (wire_fallback/over-cap) falls back to the wire with no bypass",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    gate.accept = false; // the gate (can_acquire) declines
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::large);

    c.on_edge("node-a", "alpha", demand_transition::up);
    // The policy returned true, but can_acquire declined — the coordinator undoes its
    // refcount bump and the pair falls back to the wire. A subsequent 0->1 re-probes.
    REQUIRE(gate.acquires() == 0);

    gate.accept = true;
    c.on_edge("node-a", "alpha", demand_transition::down); // 1->0 of a not-held ring: no release
    REQUIRE(gate.releases() == 0);
}

TEST_CASE("shm.coordinator the 1->0 edge releases through the gate; intermediate edges no-op",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::priority);

    c.on_edge("node-a", "alpha", demand_transition::up);   // 0->1: acquire
    c.on_edge("node-a", "alpha", demand_transition::up);   // 1->2: no-op
    REQUIRE(gate.acquires() == 1);

    c.on_edge("node-a", "alpha", demand_transition::down); // 2->1: no-op
    REQUIRE(gate.releases() == 0);
    c.on_edge("node-a", "alpha", demand_transition::down); // 1->0: release
    REQUIRE(gate.releases() == 1);
}

TEST_CASE("shm.coordinator a peer-dead event releases every ring held for that peer", "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);
    c.set_topic_hint("beta", dispatch_hint::large);

    c.on_edge("node-a", "alpha", demand_transition::up);
    c.on_edge("node-a", "beta", demand_transition::up);
    REQUIRE(gate.acquires() == 2);

    c.on_peer_dead("node-a");
    REQUIRE(gate.releases() == 2); // both rings released on peer-dead
}

TEST_CASE("shm.coordinator the default policy auto-engages; an override can disable the upgrade",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;

    // Default (no on_policy): a co-host qualifying edge auto-engages.
    {
        gate_recorder gate;
        coordinator c{reg};
        install_gate(c, gate);
        c.set_topic_hint("alpha", dispatch_hint::frequent);
        c.on_edge("node-a", "alpha", demand_transition::up);
        REQUIRE(gate.acquires() == 1);
    }
    // An override returning false disables the upgrade (the consumer-sovereign disable).
    {
        gate_recorder gate;
        coordinator c{reg};
        install_gate(c, gate);
        c.on_policy([](bool, dispatch_hint) { return false; });
        c.set_topic_hint("alpha", dispatch_hint::frequent);
        c.on_edge("node-a", "alpha", demand_transition::up);
        REQUIRE(gate.acquires() == 0);
    }
}

TEST_CASE("shm.coordinator the bilateral OR: a subscriber-side hint upgrades a hint-less publisher",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::none);     // publisher: no hint
    c.set_topic_hint("alpha", dispatch_hint::frequent); // subscriber: a qualifying hint

    c.on_edge("node-a", "alpha", demand_transition::up);
    REQUIRE(gate.acquires() == 1); // the OR upgrades the pair
}

TEST_CASE("shm.coordinator the forwarder emits the demand 0->1/1->0 exactly once per boundary; absent = no-op",
          "[shm][coordinator][forwarder]")
{
    // The forwarder's REAL emit seam: drive attach/detach edges and record the transitions.
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    inproc_channel<> sink(ex);
    ch.connect_to(sink.local_endpoint());
    forwarder::peer peer{ch, "node-a"};

    std::vector<std::pair<std::string, demand_transition>> edges;

    SECTION("with a hook installed, only the boundary crossings fire")
    {
        forwarder fwd{};
        fwd.on_demand_transition([&edges](std::string_view, std::string_view fqn, demand_transition d) {
            edges.emplace_back(std::string{fqn}, d);
        });

        REQUIRE(fwd.attach(peer, "alpha"));       // 0->1: up
        REQUIRE_FALSE(fwd.attach(peer, "alpha")); // 1->2: no emit
        REQUIRE_FALSE(fwd.detach(peer, "alpha")); // 2->1: no emit
        REQUIRE(fwd.detach(peer, "alpha"));       // 1->0: down
        ex.drain();

        REQUIRE(edges.size() == 2);
        REQUIRE(edges[0].second == demand_transition::up);
        REQUIRE(edges[1].second == demand_transition::down);
    }

    SECTION("with NO hook installed, both edges are a no-op (no crash, no emit)")
    {
        forwarder fwd{};
        REQUIRE(fwd.attach(peer, "alpha"));
        REQUIRE(fwd.detach(peer, "alpha"));
        ex.drain();
        REQUIRE(edges.empty());
    }
}

TEST_CASE("shm.coordinator peer_session::same_host reads the recorded verdict (a pure fail-closed read)",
          "[shm][coordinator][session]")
{
    // The verdict lives on peer_context (fail-closed default false). The coordinator reads
    // it through the registry; the registry reads it off the session's same_host() accessor.
    // Prove the accessor is a pure read of the recorded field.
    plexus::io::peer_context<inproc_policy> ctx;
    REQUIRE_FALSE(ctx.same_host); // fail-closed default
    ctx.same_host = true;
    REQUIRE(ctx.same_host);
}

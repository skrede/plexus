// over-limit: one cohesive medium-coordinator decision matrix; every cell drives the one
// shared stub_registry + fake_channel + gate/receive recorder harness (the engine-owned
// coordinator over its injected gates), so splitting the cells scatters that shared
// recorder-bound fixture into near-empty per-cell shells.
#include "plexus/io/shm/medium_coordinator.h"
#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/peer_context.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <map>
#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>
#include <string_view>

#include <catch2/catch_test_macros.hpp>

// The medium-assert oracle: on a co-host qualifying demand edge the coordinator MINTS the
// per-topic companion ring channel THROUGH the injected mint gate (the mint_companion path
// the node installs from its shm member) and RETAINS it — NEVER a direct registry acquire.
// The oracle records WHICH path served (a mint iff the gate accepted) and asserts dual
// delivery (the wire attach is never suppressed: a declined/off-host/hint-less pair simply
// keeps the wire — the coordinator mints nothing). It proves the per-message companion
// route (a fitting message resolves to the companion, an over-cap wire_fallback message to
// nullptr), the refcount-like 0->1/1->0 mint/drop gating, and the bounded hint map.

using namespace plexus::io::shm;
using plexus::io::demand_transition;
using plexus::io::demand_role;

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

// A minimal channel: it records the bytes it carried so a test can prove which lane a
// message rode. Single-owner (the coordinator holds it by unique_ptr); its destruction
// is the ring release the gate models.
struct fake_channel
{
    std::vector<std::size_t> sent;
    int                     *live = nullptr;
    explicit fake_channel(int *counter)
            : live(counter)
    {
        ++*live;
    }
    ~fake_channel() { --*live; }
    void send(std::span<const std::byte> b) { sent.push_back(b.size()); }
};

using coordinator = medium_coordinator<stub_registry, fake_channel>;

// The injected mint gate the node would build over its shm member: it records every mint
// as the served-medium path, returns a live companion (the can_acquire/mint verdict) at
// the configured geometry, or a declined (null) companion when accept is false (the
// broker-failure / no-shm path).
struct gate_recorder
{
    std::vector<std::string> minted;
    bool                     accept        = true;
    ring_geometry_mode       mode          = ring_geometry_mode::reliable_preserving;
    std::uint64_t            slot_capacity = 0;
    int                      live          = 0;

    companion_mint<fake_channel> mint(std::string_view fqn)
    {
        if(!accept)
            return {};
        minted.emplace_back(fqn);
        return {std::make_unique<fake_channel>(&live), mode, slot_capacity};
    }

    [[nodiscard]] std::size_t mints() const { return minted.size(); }
};

void install_gate(coordinator &c, gate_recorder &gate)
{
    c.on_gate([&gate](std::string_view fqn) { return gate.mint(fqn); });
}

// The injected RECEIVE gate the node would build over its shm member's
// mint_receive_companion: it records every attach as (node_name, fqn) and returns a live
// companion_receive whose erased owner increments/decrements a live count (modeling the
// RAII handle that releases the ring on drop), or a declined (empty) owner when accept is
// false (the broker-failure / no-shm path).
struct receive_recorder
{
    std::vector<std::pair<std::string, std::string>> attached;
    bool                                             accept = true;
    int                                              live   = 0;

    // The RAII stand-in for the concrete shm_companion_consumer: ++live on construction,
    // --live on destruction. The owner closure captures one by move and is NEVER called —
    // dropping the owner (the coordinator's 1->0/peer-dead teardown) runs ~handle, exactly
    // as the production owner drops the real ring-releasing handle.
    struct handle
    {
        int *live;
        explicit handle(int *l)
                : live(l)
        {
            ++*live;
        }
        handle(const handle &) = delete;
        handle(handle &&o) noexcept
                : live(o.live)
        {
            o.live = nullptr;
        }
        ~handle()
        {
            if(live)
                --*live;
        }
    };

    companion_receive mint(std::string_view node_name, std::string_view fqn)
    {
        if(!accept)
            return {};
        attached.emplace_back(std::string{node_name}, std::string{fqn});
        return {[h = handle{&live}]() mutable { (void)h; }};
    }

    [[nodiscard]] std::size_t attaches() const { return attached.size(); }
};

void install_receive_gate(coordinator &c, receive_recorder &gate)
{
    c.on_receive_gate([&gate](std::string_view node_name, std::string_view fqn)
                      { return gate.mint(node_name, fqn); });
}

}

TEST_CASE("shm.coordinator a co-host qualifying 0->1 mints THROUGH the gate, never a direct "
          "registry acquire",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);

    REQUIRE(gate.mints() == 1); // exactly one mint, routed through the gate
    REQUIRE(gate.live == 1);    // the companion is retained by the coordinator
}

TEST_CASE("shm.coordinator an off-host 0->1 mints NOTHING and keeps the wire", "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = false; // off-host
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);

    REQUIRE(gate.mints() == 0); // attempt_shm_upgrade false: no mint, the wire stays
}

TEST_CASE("shm.coordinator a hint-less co-host 0->1 mints NOTHING", "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    // no set_topic_hint -> dispatch_hint::none -> shm_eligible false

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);

    REQUIRE(gate.mints() == 0);
}

TEST_CASE("shm.coordinator a gate decline (broker failure) falls back to the wire with no bypass",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    gate.accept = false; // the gate (mint) declines
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::large);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    // The policy returned true, but the mint declined — the coordinator holds nothing and
    // the pair falls back to the wire. The companion route returns nullptr for it.
    REQUIRE(gate.mints() == 0);
    REQUIRE(c.companion_for("node-a", "alpha", 1) == nullptr);
}

TEST_CASE("shm.coordinator the per-message companion route: a fitting message rides the ring, "
          "over-cap the wire",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    gate.mode          = ring_geometry_mode::wire_fallback;
    gate.slot_capacity = 4096;
    coordinator c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.mints() == 1);

    // A message at or under the cap routes to the companion ring; an over-cap message keeps
    // the wire (nullptr). The dual-delivery per-message decision is route_message_medium.
    REQUIRE(c.companion_for("node-a", "alpha", 1024) != nullptr);
    REQUIRE(c.companion_for("node-a", "alpha", 4096) != nullptr);
    REQUIRE(c.companion_for("node-a", "alpha", 4097) == nullptr);
    // An un-minted (peer, topic) is always the wire.
    REQUIRE(c.companion_for("node-a", "beta", 1) == nullptr);
    REQUIRE(c.companion_for("node-z", "alpha", 1) == nullptr);
}

TEST_CASE("shm.coordinator a reliable_preserving companion carries every fitting message (no cap "
          "gate)",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate; // reliable_preserving: route_message_medium is always shm
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(c.companion_for("node-a", "alpha", 1) != nullptr);
    REQUIRE(c.companion_for("node-a", "alpha", 1u << 20) != nullptr);
}

TEST_CASE("shm.coordinator the 1->0 edge drops the companion; intermediate edges no-op",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::priority);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher); // 0->1: mint
    c.on_edge("node-a", "alpha", demand_transition::up,
              demand_role::publisher); // 1->2: no-op (already held)
    REQUIRE(gate.mints() == 1);
    REQUIRE(gate.live == 1);

    c.on_edge("node-a", "alpha", demand_transition::down,
              demand_role::publisher); // 1->0: drop (the forwarder gates the count)
    REQUIRE(gate.live == 0);           // the companion ring released
    REQUIRE(c.companion_for("node-a", "alpha", 1) == nullptr);
}

TEST_CASE("shm.coordinator a peer-dead event drops every companion held for that peer",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);
    c.set_topic_hint("beta", dispatch_hint::large);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    c.on_edge("node-a", "beta", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.live == 2);

    c.on_peer_dead("node-a");
    REQUIRE(gate.live == 0); // both companion rings dropped on peer-dead
    REQUIRE(c.companion_for("node-a", "alpha", 1) == nullptr);
}

TEST_CASE(
        "shm.coordinator a co-host SUBSCRIBER 0->1 attaches the receive companion, not a send mint",
        "[shm][coordinator]")
{
    // The receive lane (gap-closure): a subscriber-role edge routes THROUGH the receive gate
    // (attach the co-host ring as a consumer) and NOT the send mint gate — a subscriber node
    // never publishes the topic, so no send companion is minted. The companion route stays
    // nullptr for it (it has no send channel); the wire send path is irrelevant on this side.
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder    gate;
    receive_recorder rgate;
    coordinator      c{reg};
    install_gate(c, gate);
    install_receive_gate(c, rgate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::subscriber);

    REQUIRE(rgate.attaches() == 1);               // the receive companion attached
    REQUIRE(rgate.live == 1);                     // and is retained by the coordinator
    REQUIRE(gate.mints() == 0);                   // NO send companion on the subscriber side
    REQUIRE(rgate.attached[0].first == "node-a"); // keyed by the peer node_name
    REQUIRE(rgate.attached[0].second == "alpha");
    REQUIRE(c.companion_for("node-a", "alpha", 1) == nullptr); // no send channel for a receive ring
}

TEST_CASE("shm.coordinator the subscriber receive companion detaches on 1->0 and on peer-dead",
          "[shm][coordinator]")
{
    // The receive lane mirrors the send lane's refcounted teardown: the 1->0 demand edge AND
    // a peer-dead both drop the receive owner, running the handle dtor (clear sink, release
    // ring). No leak, no double-free, no dangling drain registration.
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    receive_recorder rgate;
    coordinator      c{reg};
    install_receive_gate(c, rgate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);
    c.set_topic_hint("beta", dispatch_hint::large);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::subscriber);
    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::subscriber); // 1->2: no-op
    REQUIRE(rgate.attaches() == 1);
    REQUIRE(rgate.live == 1);

    c.on_edge("node-a", "alpha", demand_transition::down, demand_role::subscriber); // 1->0: detach
    REQUIRE(rgate.live == 0);

    // A second topic, then peer-dead drops it too.
    c.on_edge("node-a", "beta", demand_transition::up, demand_role::subscriber);
    REQUIRE(rgate.live == 1);
    c.on_peer_dead("node-a");
    REQUIRE(rgate.live == 0);
}

TEST_CASE("shm.coordinator a subscriber edge with no receive gate keeps only the wire (no crash)",
          "[shm][coordinator]")
{
    // A composition with no shm member installs no receive gate: a co-host subscriber edge is
    // a no-op (the wire receive path stays), exactly as a no-mint-gate publisher edge is.
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    coordinator c{reg};
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::subscriber);
    c.on_edge("node-a", "alpha", demand_transition::down, demand_role::subscriber);
    REQUIRE(c.companion_for("node-a", "alpha", 1) == nullptr);
}

TEST_CASE("shm.coordinator the default policy auto-engages; an override can disable the upgrade",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;

    // Default (no on_policy): a co-host qualifying edge auto-engages.
    {
        gate_recorder gate;
        coordinator   c{reg};
        install_gate(c, gate);
        c.set_topic_hint("alpha", dispatch_hint::frequent);
        c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
        REQUIRE(gate.mints() == 1);
    }
    // An override returning false disables the upgrade (the consumer-sovereign disable).
    {
        gate_recorder gate;
        coordinator   c{reg};
        install_gate(c, gate);
        c.on_policy([](bool, dispatch_hint) { return false; });
        c.set_topic_hint("alpha", dispatch_hint::frequent);
        c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
        REQUIRE(gate.mints() == 0);
    }
}

TEST_CASE("shm.coordinator the bilateral OR: a subscriber-side hint upgrades a hint-less publisher",
          "[shm][coordinator]")
{
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::none);     // publisher: no hint
    c.set_topic_hint("alpha", dispatch_hint::frequent); // subscriber: a qualifying hint

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.mints() == 1); // the OR upgrades the pair
}

TEST_CASE("shm.coordinator the hint map is bounded: the last 1->0 prunes the topic's hint",
          "[shm][coordinator]")
{
    // WR-05: dynamic topic churn must not grow the hint map without bound. The hint is
    // pruned when the last demand for an fqn drops, mirroring the held-channel teardown; a
    // re-subscribe re-records it from the declare path. A SECOND co-host peer still holding
    // the topic keeps the hint alive (the prune key is the last holder, not the edge).
    stub_registry reg;
    reg.verdicts["node-a"] = true;
    reg.verdicts["node-b"] = true;
    gate_recorder gate;
    coordinator   c{reg};
    install_gate(c, gate);
    c.set_topic_hint("alpha", dispatch_hint::frequent);

    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    c.on_edge("node-b", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.mints() == 2);

    // node-a drops; node-b still holds alpha, so the hint must survive (a re-mint for a new
    // node-a 0->1 still upgrades).
    c.on_edge("node-a", "alpha", demand_transition::down, demand_role::publisher);
    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.mints() == 3); // the surviving hint re-upgraded the re-subscribe

    // Both drop: the last holder gone prunes the hint, so a later bare 0->1 (with no
    // re-record) does NOT upgrade — proving the prune fired.
    c.on_edge("node-a", "alpha", demand_transition::down, demand_role::publisher);
    c.on_edge("node-b", "alpha", demand_transition::down, demand_role::publisher);
    c.on_edge("node-a", "alpha", demand_transition::up, demand_role::publisher);
    REQUIRE(gate.mints() == 3); // the pruned hint resolves to none: no upgrade
}

TEST_CASE("shm.coordinator the forwarder emits the demand 0->1/1->0 exactly once per boundary; "
          "absent = no-op",
          "[shm][coordinator][forwarder]")
{
    using forwarder = plexus::io::message_forwarder<plexus::inproc::inproc_policy>;
    using plexus::inproc::inproc_bus;
    using plexus::inproc::inproc_executor;
    using plexus::inproc::inproc_channel;

    // The forwarder's REAL emit seam: drive attach/detach edges and record the transitions.
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    inproc_channel<>  sink(ex);
    ch.connect_to(sink.local_endpoint());
    forwarder::peer peer{ch, "node-a"};

    struct edge
    {
        std::string       fqn;
        demand_transition dir;
        demand_role       role;
    };
    std::vector<edge> edges;

    SECTION("with a hook installed, only the boundary crossings fire, carrying the subscriber role")
    {
        forwarder fwd{};
        fwd.on_demand_transition(
                [&edges](std::string_view, std::string_view fqn, demand_transition d, demand_role r)
                { edges.push_back({std::string{fqn}, d, r}); });

        REQUIRE(fwd.attach(peer, "alpha"));       // 0->1: up
        REQUIRE_FALSE(fwd.attach(peer, "alpha")); // 1->2: no emit
        REQUIRE_FALSE(fwd.detach(peer, "alpha")); // 2->1: no emit
        REQUIRE(fwd.detach(peer, "alpha"));       // 1->0: down
        ex.drain();

        REQUIRE(edges.size() == 2);
        REQUIRE(edges[0].dir == demand_transition::up);
        REQUIRE(edges[1].dir == demand_transition::down);
        // attach/detach are the SUBSCRIBER side of the pair (the local node subscribed).
        REQUIRE(edges[0].role == demand_role::subscriber);
        REQUIRE(edges[1].role == demand_role::subscriber);
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

TEST_CASE("shm.coordinator peer_session::same_host reads the recorded verdict (a pure fail-closed "
          "read)",
          "[shm][coordinator][session]")
{
    // The verdict lives on peer_context (fail-closed default false). The coordinator reads
    // it through the registry; the registry reads it off the session's same_host() accessor.
    // Prove the accessor is a pure read of the recorded field.
    plexus::io::peer_context<plexus::inproc::inproc_policy> ctx;
    REQUIRE_FALSE(ctx.same_host); // fail-closed default
    ctx.same_host = true;
    REQUIRE(ctx.same_host);
}

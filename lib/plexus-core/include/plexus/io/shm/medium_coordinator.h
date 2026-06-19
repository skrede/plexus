#ifndef HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H

#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/demand_transition.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace plexus::io::shm {

// What the node's mint gate hands back for a (peer, topic): the live companion ring
// channel (single-owner, retained by the coordinator) plus the resolved per-message
// route inputs (the ring mode + the capped slot capacity). A null channel signals the
// gate DECLINED — the pair keeps the wire (a broker failure, or no shm member). The
// channel's type is the engine's byte_channel (a polymorphic erasure in a multi-
// transport node), wrapping the shm member's live shm_byte_channel.
template <typename Channel>
struct companion_mint
{
    std::unique_ptr<Channel> channel;
    ring_geometry_mode       mode          = ring_geometry_mode::reliable_preserving;
    std::uint64_t            slot_capacity = 0;
};

// What the node's receive gate hands back for a co-host SUBSCRIBER (peer, topic): a type-
// erased OWNER of the live receive companion. The owner is never called — it exists solely
// to hold the concrete shm_companion_consumer RAII handle by move, so dropping it (on
// 1->0 / peer-dead) runs the handle's destructor (clear the consumer sink, release the
// ring). An empty owner signals the gate DECLINED (a broker failure, or no shm member) — the
// pair then has only the wire receive path. This keeps the coordinator decoupled from the
// concrete member's handle type (the same erasure discipline companion_mint uses for send).
struct companion_receive
{
    plexus::detail::move_only_function<void()> owner;

    [[nodiscard]] bool engaged() const noexcept { return static_cast<bool>(owner); }
};

// The per-(peer, topic) medium-selection coordinator: the JOIN of three layers no
// existing type sees — peer locality (the session's same_host verdict), the per-(peer,
// fqn) demand 0->1/1->0 transition (the forwarder), and medium provisioning (the
// shm-capable transport). It is engine-owned and BORROWS the peer-session registry by
// reference (the same_host read); it OWNS the live companion ring channels it mints (no
// singleton, no shared_from_this — the vagus discipline, shm_selection.h:98-104).
//
// It consumes the forwarder's demand transition: on a 0->1 (up) it reads the peer's
// same_host verdict, derives the topic's own dispatch_hint, runs the upgrade-policy
// hook (default attempt_shm_upgrade), and — on a co-host qualifying topic — MINTS the
// per-topic companion ring channel as an ADDITIVE dual-delivery lane THROUGH the injected
// mint gate (over the shm member's mint_companion), retaining it for the publish fan's
// per-message companion route. The wire attach is NEVER suppressed: a declined/off-host/
// hint-less pair simply keeps the wire (D-08). The 1->0 (down) and a peer-dead event drop
// the channels the peer held (the channel's destructor releases the ring refcount).
//
// The mint gate is injected as one predicate (the prefer_shm_hook seed constraint: the
// coordinator stays decoupled from the concrete shm member type). A composition with no
// shm member leaves it unset, so the coordinator is inert (one predictable branch).
template <typename Registry, typename Channel>
class medium_coordinator
{
public:
    explicit medium_coordinator(Registry &registry) noexcept : m_registry(registry) {}

    medium_coordinator(const medium_coordinator &) = delete;
    medium_coordinator &operator=(const medium_coordinator &) = delete;

    // The mint gate the acquire routes THROUGH — installed by the node only when the
    // composition carries an shm member (else the coordinator stays inert). It mints the
    // companion ring channel for an fqn (a null channel = declined). The coordinator
    // RETAINS the returned channel; releasing it (on 1->0 / peer-dead) drops the ring.
    void on_gate(plexus::detail::move_only_function<companion_mint<Channel>(std::string_view)> mint)
    {
        m_mint = std::move(mint);
    }

    // The receive-companion gate the SUBSCRIBER role routes THROUGH — installed by the node
    // only when the composition carries an shm member (else the subscriber keeps only the
    // wire receive path). It attaches the co-host request-direction ring as a consumer and
    // routes drained framed messages into the receive path for (node_name, fqn), returning
    // a type-erased owner of the live receive companion (empty = declined). The coordinator
    // RETAINS the owner; dropping it (on 1->0 / peer-dead) tears the receive lane down. The
    // node_name rides the mint so the node wires the drained frames into the matching peer
    // session's receive entry (the same one the wire feeds).
    void on_receive_gate(
        plexus::detail::move_only_function<companion_receive(std::string_view, std::string_view)> mint)
    {
        m_receive_mint = std::move(mint);
    }

    // The required-with-default upgrade-policy hook: when unset the default IS
    // attempt_shm_upgrade(same_host, own_hint). An operator-supplied hook overrides it
    // (it can only DECLINE — attempt_shm_upgrade still gates on same_host inside it).
    void on_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_policy = std::move(policy);
    }

    // Record a topic's own dispatch_hint (the publisher AND subscriber declare paths each
    // thread theirs). The bilateral OR: EITHER end's hint upgrades the pair, so the bits
    // accumulate per fqn. An fqn with no recorded hint resolves to none (not shm-eligible).
    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    // The consume body installed into the forwarder's on_demand_transition. The role
    // decides which lane the up-edge engages: a SUBSCRIBER edge attaches the receive
    // companion (drain the co-host ring into the receive path), a PUBLISHER edge mints the
    // send companion (the publish fan routes fitting messages over it). The down-edge drops
    // whichever lane this (peer, fqn) held — both teardown paths are keyed the same way.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir,
                 demand_role role)
    {
        if(dir == demand_transition::up)
            on_up(node_name, fqn, role);
        else
            on_down(node_name, fqn);
    }

    // The publish fan's per-message companion route (the forwarder's on_companion_route
    // body): return the retained companion channel for (node_name, fqn) when this message
    // fits its medium decision (route_message_medium == shm), else nullptr so the message
    // keeps the recorded wire sub.channel (the dual-delivery fail-safe). An unheld pair, or
    // an over-cap wire_fallback message, both resolve to nullptr.
    [[nodiscard]] Channel *companion_for(std::string_view node_name, std::string_view fqn,
                                         std::size_t bytes)
    {
        const ring *r = find_ring(node_name, fqn);
        if(r == nullptr)
            return nullptr;
        return route_message_medium(r->mode, bytes, r->slot_capacity) == same_host_medium::shm
                   ? r->channel.get()
                   : nullptr;
    }

    // Release every ring the peer holds (the peer-dead teardown): drop each held channel
    // (its destructor releases the ring), then forget the peer's entries.
    void on_peer_dead(std::string_view node_name)
    {
        m_held.erase(std::string{node_name});
    }

private:
    // One held same-host lane for a (peer, fqn). A PUBLISHER edge fills `channel` (the send
    // companion the publish fan routes over) + its {mode, slot_capacity}; a SUBSCRIBER edge
    // fills `receive` (the type-erased owner of the live receive companion). Exactly one is
    // engaged per record — the role at the up-edge decides — and dropping the record tears
    // down whichever it holds (the send channel dtor / the receive owner dtor releases the
    // ring). The two lanes are keyed identically, so find_ring/on_down treat them uniformly.
    struct ring
    {
        std::string              fqn;
        std::unique_ptr<Channel> channel;       // the send companion (publisher role)
        ring_geometry_mode       mode          = ring_geometry_mode::reliable_preserving;
        std::uint64_t            slot_capacity = 0;
        companion_receive        receive;        // the receive companion owner (subscriber role)
    };
    using ring_list = std::vector<ring>;

    void on_up(std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(find_ring(node_name, fqn) != nullptr)
            return;
        if(!run_policy(m_registry.same_host_for(node_name), hint_for(fqn)))
            return;
        if(role == demand_role::subscriber)
            attach_receive(node_name, fqn);
        else
            mint_send(node_name, fqn);
    }

    // The subscriber lane: attach the co-host ring as a consumer (drain into the receive
    // path) through the receive gate. A declined gate (broker failure, or no shm member)
    // keeps the wire receive path — nothing is retained.
    void attach_receive(std::string_view node_name, std::string_view fqn)
    {
        if(!m_receive_mint)
            return;
        companion_receive received = m_receive_mint(node_name, fqn);
        if(!received.engaged())
            return;
        ring r;
        r.fqn     = std::string{fqn};
        r.receive = std::move(received);
        m_held[std::string{node_name}].push_back(std::move(r));
    }

    // The publisher lane: mint the send companion through the mint gate (the publish fan's
    // per-message route consults it). A declined gate keeps the wire send path.
    void mint_send(std::string_view node_name, std::string_view fqn)
    {
        if(!m_mint)
            return;
        companion_mint<Channel> minted = m_mint(fqn);
        if(!minted.channel)
            return; // gate declined (broker failure): the wire stays
        ring r;
        r.fqn           = std::string{fqn};
        r.channel       = std::move(minted.channel);
        r.mode          = minted.mode;
        r.slot_capacity = minted.slot_capacity;
        m_held[std::string{node_name}].push_back(std::move(r));
    }

    void on_down(std::string_view node_name, std::string_view fqn)
    {
        auto it = m_held.find(std::string{node_name});
        if(it == m_held.end())
            return;
        std::erase_if(it->second, [&](const ring &r) { return r.fqn == fqn; });
        if(it->second.empty())
            m_held.erase(it);
        // The last demand for the fqn is gone: drop its hint so a churn of distinct topic
        // names cannot grow m_hints without bound (the demand-edge prune mirrors the held
        // teardown; a re-subscribe re-records the hint from the declare path).
        if(!any_holder(fqn))
            m_hints.erase(std::string{fqn});
    }

    [[nodiscard]] bool run_policy(bool same_host, dispatch_hint own_hint)
    {
        return m_policy ? m_policy(same_host, own_hint) : attempt_shm_upgrade(same_host, own_hint);
    }

    [[nodiscard]] dispatch_hint hint_for(std::string_view fqn) const
    {
        auto it = m_hints.find(std::string{fqn});
        return it == m_hints.end() ? dispatch_hint::none : it->second;
    }

    [[nodiscard]] const ring *find_ring(std::string_view node_name, std::string_view fqn) const
    {
        auto it = m_held.find(std::string{node_name});
        if(it == m_held.end())
            return nullptr;
        for(const ring &r : it->second)
            if(r.fqn == fqn)
                return &r;
        return nullptr;
    }

    [[nodiscard]] bool any_holder(std::string_view fqn) const
    {
        for(const auto &[name, rings] : m_held)
            for(const ring &r : rings)
                if(r.fqn == fqn)
                    return true;
        return false;
    }

    Registry &m_registry;
    std::unordered_map<std::string, ring_list> m_held;
    std::unordered_map<std::string, dispatch_hint> m_hints;
    plexus::detail::move_only_function<companion_mint<Channel>(std::string_view)> m_mint;
    plexus::detail::move_only_function<companion_receive(std::string_view, std::string_view)> m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
};

}

#endif

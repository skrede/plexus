#ifndef HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H

#include "plexus/io/shm/ring_geometry_mode.h"
#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/shm/detail/coordinator_rings.h"
#include "plexus/io/demand_transition.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io::shm {

// A null channel signals the gate DECLINED — the pair keeps the wire. The coordinator
// retains the channel single-owner; mode + slot_capacity are the per-message route inputs.
template<typename Channel>
struct companion_mint
{
    std::unique_ptr<Channel> channel;
    ring_geometry_mode       mode          = ring_geometry_mode::reliable_preserving;
    std::uint64_t            slot_capacity = 0;
};

// A type-erased OWNER of the live receive companion: never called, it exists only to hold
// the concrete RAII handle by move, so dropping it runs the handle's destructor (release the
// ring). An empty owner signals the gate DECLINED — the pair then has only the wire receive
// path. The erasure keeps the coordinator decoupled from the concrete member's handle type.
struct companion_receive
{
    plexus::detail::move_only_function<void()> owner;

    [[nodiscard]] bool engaged() const noexcept { return static_cast<bool>(owner); }
};

// The per-(peer, topic) medium-selection coordinator: the JOIN of peer locality (the
// session's same_host verdict), the per-(peer, fqn) demand up/down transition, and medium
// provisioning. Engine-owned; it BORROWS the peer-session registry by reference and OWNS the
// live companion ring channels it mints (no singleton, no shared_from_this — see
// shm_selection.h). The companion lane is ADDITIVE: the wire attach is NEVER suppressed, so a
// declined / off-host / hint-less pair simply keeps the wire. The gate is injected as one
// predicate; a composition with no shm member leaves it unset, so the coordinator is inert.
template<typename Registry, typename Channel>
class medium_coordinator
{
public:
    explicit medium_coordinator(Registry &registry) noexcept
            : m_registry(registry)
    {
    }

    medium_coordinator(const medium_coordinator &)            = delete;
    medium_coordinator &operator=(const medium_coordinator &) = delete;

    // Mints the send companion for an fqn (null channel = declined). The coordinator retains
    // it; releasing it (on down / peer-dead) drops the ring. Unset = inert (no shm member).
    void on_gate(plexus::detail::move_only_function<companion_mint<Channel>(std::string_view)> mint)
    {
        m_mint = std::move(mint);
    }

    // The SUBSCRIBER lane gate: attaches the co-host ring as a consumer and routes drained
    // framed messages into (node_name, fqn)'s receive path — the same entry the wire feeds —
    // returning a retained owner of the live receive companion (empty = declined). Unset =
    // the subscriber keeps only the wire receive path.
    void on_receive_gate(plexus::detail::move_only_function<companion_receive(std::string_view,
                                                                              std::string_view)>
                                 mint)
    {
        m_receive_mint = std::move(mint);
    }

    // Override can only DECLINE; attempt_shm_upgrade still gates on same_host.
    void on_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_policy = std::move(policy);
    }

    // Bilateral OR: EITHER end's hint upgrades the pair, so the bits accumulate per fqn. An
    // fqn with no recorded hint resolves to none (not shm-eligible).
    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    // The forwarder's on_demand_transition body. The up-edge role decides the lane (subscriber
    // attaches the receive companion, publisher mints the send companion); the down-edge drops
    // whichever lane this (peer, fqn) held — both teardown paths key the same way.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir,
                 demand_role role)
    {
        if(dir == demand_transition::up)
            on_up(node_name, fqn, role);
        else
            on_down(node_name, fqn);
    }

    // The publish fan's per-message route: the retained companion channel for (node_name, fqn)
    // when this message fits the medium decision, else nullptr so it keeps the wire sub.channel
    // (the dual-delivery fail-safe). An unheld pair or an over-cap message resolves to nullptr.
    [[nodiscard]] Channel *companion_for(std::string_view node_name, std::string_view fqn,
                                         std::size_t bytes)
    {
        return detail::route_companion(m_held, node_name, fqn, bytes);
    }

    // Peer-dead teardown: dropping the held channels runs their destructors (releasing the ring).
    void on_peer_dead(std::string_view node_name) { m_held.erase(std::string{node_name}); }

private:
    using ring      = detail::coordinator_ring<Channel, companion_receive>;
    using ring_list = std::vector<ring>;

    void on_up(std::string_view node_name, std::string_view fqn, demand_role role)
    {
        if(detail::find_ring(m_held, node_name, fqn) != nullptr)
            return;
        if(!run_policy(m_registry.same_host_for(node_name), hint_for(fqn)))
            return;
        if(role == demand_role::subscriber)
            attach_receive(node_name, fqn);
        else
            mint_send(node_name, fqn);
    }

    // A declined gate keeps the wire receive path — nothing is retained.
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

    // A declined gate keeps the wire send path — nothing is retained.
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
        detail::erase_ring(m_held, node_name, fqn);
        // Last demand for the fqn gone: drop its hint so a churn of distinct topic names
        // cannot grow m_hints without bound (a re-subscribe re-records it from the declare path).
        if(!detail::any_holder(m_held, fqn))
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

    Registry                                                                     &m_registry;
    std::unordered_map<std::string, ring_list>                                    m_held;
    std::unordered_map<std::string, dispatch_hint>                                m_hints;
    plexus::detail::move_only_function<companion_mint<Channel>(std::string_view)> m_mint;
    plexus::detail::move_only_function<companion_receive(std::string_view, std::string_view)>
                                                                  m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
};

}

#endif

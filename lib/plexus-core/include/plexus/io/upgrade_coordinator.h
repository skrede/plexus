#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/dispatch_hint.h"
#include "plexus/io/detail/upgrade_rings.h"
#include "plexus/io/demand_transition.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The per-(peer, topic) medium-selection coordinator: the JOIN of peer locality (the session's
// same_host verdict), the per-(peer, fqn) demand up/down transition, and medium provisioning.
// Engine-owned; it BORROWS the peer-session registry by reference and OWNS the live companion
// ring channels it mints (no singleton, no shared_from_this). The companion lane is ADDITIVE: the
// wire attach is NEVER suppressed, so a declined / off-host / hint-less pair simply keeps the
// wire. The gate is injected as one predicate; a composition with no upgrade member leaves it
// unset, so the coordinator is inert. The coordinator names no transport: it carries the
// dispatch_hint opaquely and routes per-message through an injected predicate the medium builds.
template<typename Registry, typename Channel>
class upgrade_coordinator
{
public:
    explicit upgrade_coordinator(Registry &registry) noexcept
            : m_registry(registry)
    {
    }

    upgrade_coordinator(const upgrade_coordinator &)            = delete;
    upgrade_coordinator &operator=(const upgrade_coordinator &) = delete;

    // Mints the send companion for an fqn (null channel = declined). The coordinator retains
    // it; releasing it (on down / peer-dead) drops the ring. Unset = inert (no upgrade member).
    void on_gate(plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)> mint)
    {
        m_mint = std::move(mint);
    }

    // The SUBSCRIBER lane gate: attaches the co-host ring as a consumer and routes drained
    // framed messages into (node_name, fqn)'s receive path — the same entry the wire feeds —
    // returning a retained owner of the live receive companion (empty = declined). Unset =
    // the subscriber keeps only the wire receive path.
    void on_receive_gate(plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> mint)
    {
        m_receive_mint = std::move(mint);
    }

    // An injected medium policy reads the hint bits to decide an upgrade. Unset = the generic
    // default (same_host && any hint bit set). The override can only further DECLINE — the
    // default already gates on same_host.
    void on_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_policy = std::move(policy);
    }

    // Bilateral OR: EITHER end's hint upgrades the pair, so the bits accumulate per fqn. An
    // fqn with no recorded hint resolves to none (no bit set).
    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    // The forwarder's on_demand_transition body. The up-edge role decides the lane (subscriber
    // attaches the receive companion, publisher mints the send companion); the down-edge drops
    // whichever lane this (peer, fqn) held — both teardown paths key the same way.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
    {
        if(dir == demand_transition::up)
            on_up(node_name, fqn, role);
        else
            on_down(node_name, fqn);
    }

    // The publish fan's per-message route: the retained companion channel for (node_name, fqn)
    // when this message fits the medium decision, else nullptr so it keeps the wire sub.channel
    // (the dual-delivery fail-safe). An unheld pair or an over-cap message resolves to nullptr.
    [[nodiscard]] Channel *companion_for(std::string_view node_name, std::string_view fqn, std::size_t bytes)
    {
        return detail::route_companion(m_held, node_name, fqn, bytes);
    }

    // Peer-dead teardown: dropping the held channels runs their destructors (releasing the ring).
    void on_peer_dead(std::string_view node_name)
    {
        m_held.erase(std::string{node_name});
    }

private:
    using ring      = detail::coordinator_ring<Channel, upgrade_receive>;
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
        upgrade_receive received = m_receive_mint(node_name, fqn);
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
        upgrade_mint<Channel> minted = m_mint(fqn);
        if(!minted.channel)
            return; // gate declined (broker failure): the wire stays
        ring r;
        r.fqn     = std::string{fqn};
        r.channel = std::move(minted.channel);
        r.fits    = std::move(minted.fits);
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
        return m_policy ? m_policy(same_host, own_hint) : (same_host && any_set(own_hint));
    }

    [[nodiscard]] dispatch_hint hint_for(std::string_view fqn) const
    {
        auto it = m_hints.find(std::string{fqn});
        return it == m_hints.end() ? dispatch_hint::none : it->second;
    }

    Registry                                                                               &m_registry;
    std::unordered_map<std::string, ring_list>                                              m_held;
    std::unordered_map<std::string, dispatch_hint>                                          m_hints;
    plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)>             m_mint;
    plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)>                           m_policy;
};

}

#endif

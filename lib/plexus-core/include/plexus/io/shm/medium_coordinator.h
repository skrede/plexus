#ifndef HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_SHM_MEDIUM_COORDINATOR_H

#include "plexus/io/shm/shm_selection.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/demand_transition.h"

#include "plexus/detail/compat.h"

#include <string>
#include <vector>
#include <utility>
#include <algorithm>
#include <string_view>
#include <unordered_map>

namespace plexus::io::shm {

// The per-(peer, topic) medium-selection coordinator: the JOIN of three layers no
// existing type sees — peer locality (the session's same_host verdict), the per-(peer,
// fqn) demand 0->1/1->0 transition (the forwarder), and medium provisioning (the
// shm-capable transport). It is engine-owned and BORROWS the peer-session registry by
// reference (the same_host read); it OWNS only its acquired_ring_set refcount (no
// singleton, no shared_from_this — the vagus discipline, shm_selection.h:98-104).
//
// It consumes the forwarder's demand transition: on a 0->1 (up) it reads the peer's
// same_host verdict, derives the topic's own dispatch_hint, runs the upgrade-policy
// hook (default attempt_shm_upgrade), and — on a co-host qualifying topic — acquires the
// per-topic ring as an ADDITIVE dual-delivery lane THROUGH the injected gate probe
// (can_acquire/prefer_shm_hook), never a direct registry acquire. The wire attach is
// NEVER suppressed: a declined/off-host/hint-less pair simply keeps the wire (D-08). The
// 1->0 (down) and a peer-dead event release the rings the peer held, through the same
// gate.
//
// The gate is injected as two predicates (the prefer_shm_hook seed constraint: the
// coordinator stays decoupled from the concrete shm member type). A composition with no
// shm member leaves them unset, so the coordinator is inert (one predictable branch).
template <typename Registry>
class medium_coordinator
{
public:
    explicit medium_coordinator(Registry &registry) noexcept : m_registry(registry) {}

    medium_coordinator(const medium_coordinator &) = delete;
    medium_coordinator &operator=(const medium_coordinator &) = delete;

    // The gate the acquire/release routes THROUGH — installed by the node only when the
    // composition carries an shm member (else the coordinator stays inert). acquire
    // returns whether the ring was acquired (can_acquire accepted); release drops a held
    // bump (abandon). Both key on the fqn (the ring's deterministic region name derives
    // from it).
    void on_gate(plexus::detail::move_only_function<bool(std::string_view)> acquire,
                 plexus::detail::move_only_function<void(std::string_view)> release)
    {
        m_gate_acquire = std::move(acquire);
        m_gate_release = std::move(release);
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

    // The consume body installed into the forwarder's on_demand_transition.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir)
    {
        if(dir == demand_transition::up)
            on_up(node_name, fqn);
        else
            on_down(node_name, fqn);
    }

    // Release every ring the peer holds (the peer-dead teardown): issue the per-ring gate
    // release for each held fqn, then forget the peer's refcount entries.
    void on_peer_dead(std::string_view node_name)
    {
        auto it = m_held.find(std::string{node_name});
        if(it == m_held.end())
            return;
        for(const std::string &fqn : it->second)
            if(m_gate_release)
                m_gate_release(fqn);
        m_rings.forget(node_name);
        m_held.erase(it);
    }

private:
    void on_up(std::string_view node_name, std::string_view fqn)
    {
        if(!m_gate_acquire)
            return;
        const bool sh = m_registry.same_host_for(node_name);
        if(!run_policy(sh, hint_for(fqn)))
            return;
        if(m_rings.acquire(node_name, fqn) != 1u)
            return;
        if(m_gate_acquire(fqn))
            m_held[std::string{node_name}].emplace_back(fqn);
        else
            m_rings.release(node_name, fqn); // gate declined: undo the bump, the wire stays
    }

    void on_down(std::string_view node_name, std::string_view fqn)
    {
        if(m_rings.release(node_name, fqn) != 0u)
            return;
        if(m_gate_release)
            m_gate_release(fqn);
        forget_held(node_name, fqn);
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

    void forget_held(std::string_view node_name, std::string_view fqn)
    {
        auto it = m_held.find(std::string{node_name});
        if(it == m_held.end())
            return;
        std::erase(it->second, std::string{fqn});
        if(it->second.empty())
            m_held.erase(it);
    }

    Registry &m_registry;
    acquired_ring_set m_rings;
    std::unordered_map<std::string, std::vector<std::string>> m_held;
    std::unordered_map<std::string, dispatch_hint> m_hints;
    plexus::detail::move_only_function<bool(std::string_view)> m_gate_acquire;
    plexus::detail::move_only_function<void(std::string_view)> m_gate_release;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include "plexus/io/detail/upgrade_lanes.h"
#include "plexus/io/detail/upgrade_rings.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The per-(peer, topic) medium-selection coordinator: the JOIN of peer locality (the session's
// same_host verdict), the per-(peer, fqn) demand transition, the pair's companion events and medium
// provisioning. Engine-owned; it BORROWS the peer-session registry and OWNS the companion lanes it
// holds. A lane is ADDITIVE: the wire attach is NEVER suppressed, so a declined, off-host or
// unanswered pair keeps the wire. Shared memory engages only when both ends' policies allow it: the
// publisher offers the ring it minted, the subscriber attaches it and answers ready, and only then
// does the publisher route over it. The coordinator names no transport; with no gate installed it
// is inert.
template<typename Registry, typename Channel>
class upgrade_coordinator
{
public:
    explicit upgrade_coordinator(Registry &registry) noexcept
            : m_registry(registry)
            , m_fallbacks()
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

    // The node's own publisher and subscriber hints accumulate per fqn; no hint rides the wire. An
    // fqn with no recorded hint resolves to none (no bit set).
    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    // The forwarder's on_demand_transition body. A publisher's up-edge mints its send lane; a
    // subscriber's records the demand its publisher's offer may attach. The down-edge drops the lane.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
    {
        if(dir == demand_transition::up)
            detail::upgrade_lanes::up(*this, node_name, fqn, role);
        else
            detail::upgrade_lanes::down(*this, node_name, fqn, role);
    }

    void on_companion(std::string_view node_name, std::string_view fqn, companion_event event)
    {
        if(event == companion_event::offered || event == companion_event::not_offered)
            detail::upgrade_lanes::offer(*this, node_name, fqn, event == companion_event::offered);
        else
            detail::upgrade_lanes::answer(*this, node_name, fqn, event);
    }

    // The subscriber's ready and withdrawn answers leave through this; unset, nothing is answered.
    void on_companion_send(plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event)> send)
    {
        m_companion_send_cb = std::move(send);
    }

    bool offers(std::string_view node_name, std::string_view fqn) const
    {
        return detail::find_ring(m_held, node_name, fqn, demand_role::publisher) != nullptr;
    }

    // The publish fan's per-message route: the ready lane's channel for (node_name, fqn) when this
    // message fits the medium decision, else nullptr so it keeps the wire sub.channel.
    Channel *companion_for(std::string_view node_name, std::string_view fqn, std::size_t bytes)
    {
        return detail::route_companion(m_held, node_name, fqn, bytes);
    }

    // Arms a wake after the delay; the owner binds it to one timer whose expiry calls on_retry_due. Unset, a transient
    // decline is final.
    void set_retry_scheduler(plexus::detail::move_only_function<void(std::chrono::milliseconds)> schedule)
    {
        m_schedule_retry_cb = std::move(schedule);
    }

    void on_retry_due()
    {
        detail::upgrade_lanes::retry(*this);
    }

    // Peer-dead teardown: dropping the held channels runs their destructors (releasing the ring).
    void on_peer_dead(std::string_view node_name)
    {
        m_retry.erase_peer(node_name);
        m_held.erase(std::string{node_name});
    }

    // The medium can no longer serve the fqn: every lane for it drops back to the wire and each publisher this node read
    // from is told. The hint stays so that a later up-edge or offer can still upgrade.
    void on_medium_lost(std::string_view fqn)
    {
        detail::upgrade_lanes::lost(*this, fqn);
    }

    // Every declined attempt is counted here, and so is each demotion or loss the medium reports for a lane it holds.
    void count_fallback(fallback_reason reason) noexcept
    {
        if(reason != fallback_reason::none)
            ++m_fallbacks[static_cast<std::size_t>(reason)];
    }

    const fallback_counts &fallbacks() const noexcept
    {
        return m_fallbacks;
    }

private:
    Registry &m_registry;
    fallback_counts m_fallbacks;
    detail::upgrade_retry m_retry;
    detail::coordinator_held<Channel, upgrade_receive> m_held;
    std::unordered_map<std::string, dispatch_hint> m_hints;
    plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)> m_mint;
    plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
    plexus::detail::move_only_function<void(std::chrono::milliseconds)> m_schedule_retry_cb;
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event)> m_companion_send_cb;

    friend struct detail::upgrade_lanes;

    bool run_policy(bool same_host, dispatch_hint own_hint)
    {
        return m_policy ? m_policy(same_host, own_hint) : (same_host && any_set(own_hint));
    }

    dispatch_hint hint_for(std::string_view fqn) const
    {
        auto it = m_hints.find(std::string{fqn});
        return it == m_hints.end() ? dispatch_hint::none : it->second;
    }
};

}

#endif

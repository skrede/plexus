#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include "plexus/io/detail/upgrade_lanes.h"
#include "plexus/io/detail/upgrade_send_lanes.h"
#include "plexus/io/detail/upgrade_receive_lanes.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <string>
#include <cstddef>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::io {

// The per-(peer, topic) medium-selection coordinator: the JOIN of peer locality (the session's same_host verdict), the
// per-(peer, fqn) demand transition, the pair's companion events and medium provisioning. Engine-owned; it BORROWS the
// peer-session registry and OWNS the lanes it holds. A lane is ADDITIVE: the wire attach is NEVER suppressed, so a
// declined, off-host or unanswered pair keeps the wire. Shared memory engages only when both ends' policies allow it:
// the publisher offers its one lane for the topic, the subscriber attaches it and answers ready, and only then does the
// publisher route that subscriber over it. The coordinator names no transport; with no gate installed it is inert.
template<typename Registry, typename Channel>
class upgrade_coordinator
{
public:
    using channel_type = Channel;

    explicit upgrade_coordinator(Registry &registry) noexcept
            : m_registry(registry)
            , m_fallbacks()
    {
    }

    upgrade_coordinator(const upgrade_coordinator &)            = delete;
    upgrade_coordinator &operator=(const upgrade_coordinator &) = delete;

    // Mints the send lane for an fqn (null channel = declined). Unset = inert (no upgrade member).
    void on_gate(plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)> mint)
    {
        m_mint = std::move(mint);
    }

    // The SUBSCRIBER lane gate: attaches the co-host ring as a consumer and routes drained framed messages into
    // (node_name, fqn)'s receive path, the same entry the wire feeds (empty owner = declined).
    void on_receive_gate(plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> mint)
    {
        m_receive_mint = std::move(mint);
    }

    // Unset = the generic default (same_host && any hint bit set).
    void on_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_policy = std::move(policy);
    }

    // The node's own publisher and subscriber hints accumulate per fqn; no hint rides the wire.
    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        auto [it, inserted] = m_hints.try_emplace(std::string{fqn}, hint);
        if(!inserted)
            it->second = it->second | hint;
    }

    // A publisher's up-edge offers the peer its send lane; a subscriber's records the demand its publisher's offer may
    // attach. The down-edge drops the peer's hold.
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

    // Runs with the channel of a lane about to be destroyed, on every path that destroys one.
    void on_lane_released(plexus::detail::move_only_function<void(Channel &)> released)
    {
        m_lane_released_cb = std::move(released);
    }

    bool offers(std::string_view node_name, std::string_view fqn) const
    {
        return detail::offered(m_send, node_name, fqn);
    }

    // The lane's channel when the peer answered ready and this message fits, else nullptr so it keeps the wire.
    Channel *companion_for(std::string_view node_name, std::string_view fqn, std::size_t bytes)
    {
        return detail::route_send_lane(m_send, node_name, fqn, bytes);
    }

    Channel *lane_for(std::string_view fqn) const
    {
        return detail::send_lane_channel(m_send, fqn, false);
    }

    // The node's own subscriber's hold on the lane, minted if absent; nullptr when the gate declined.
    Channel *hold_self(std::string_view fqn)
    {
        return detail::upgrade_send_lanes::hold_self(*this, fqn);
    }

    void release_self(std::string_view fqn)
    {
        detail::upgrade_send_lanes::release_self(*this, fqn);
    }

    Channel *self_channel(std::string_view fqn) const
    {
        return detail::send_lane_channel(m_send, fqn, true);
    }

    // The owner binds this to one timer whose expiry calls on_retry_due. Unset, a transient decline is final.
    void set_retry_scheduler(plexus::detail::move_only_function<void(std::chrono::milliseconds)> schedule)
    {
        m_schedule_retry_cb = std::move(schedule);
    }

    void on_retry_due()
    {
        detail::upgrade_lanes::retry(*this);
    }

    void on_peer_dead(std::string_view node_name)
    {
        detail::upgrade_send_lanes::forget_peer(*this, node_name);
    }

    // Every lane for the fqn drops back to the wire and each publisher this node read from is told; the hint stays.
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
    detail::receive_lanes m_receive;
    detail::send_lanes<Channel> m_send;
    std::unordered_map<std::string, dispatch_hint> m_hints;
    plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)> m_mint;
    plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
    plexus::detail::move_only_function<void(std::chrono::milliseconds)> m_schedule_retry_cb;
    plexus::detail::move_only_function<void(Channel &)> m_lane_released_cb;
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event)> m_companion_send_cb;

    friend struct detail::upgrade_lanes;
    friend struct detail::upgrade_send_lanes;

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

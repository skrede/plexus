#ifndef HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H
#define HPP_GUARD_PLEXUS_IO_UPGRADE_COORDINATOR_H

#include "plexus/io/dispatch_hint.h"
#include "plexus/io/companion_event.h"
#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include "plexus/io/detail/upgrade_hints.h"
#include "plexus/io/detail/upgrade_lanes.h"
#include "plexus/io/detail/upgrade_offers.h"
#include "plexus/io/detail/upgrade_send_lanes.h"
#include "plexus/io/detail/upgrade_receive_lanes.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::io {

// The per-(peer, topic) medium coordinator over peer locality, demand edges, companion events and provisioning.
// Engine-owned; it BORROWS the peer-session registry and OWNS its lanes, which are ADDITIVE: the wire attach is NEVER
// suppressed. A publisher offers its one lane for a topic it publishes; the subscriber attaches it and answers ready,
// and only then is it routed over the lane, so both ends' policies must allow it. With no gate installed it is inert.
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

    // Attaches a co-host ring as a consumer feeding (node_name, fqn)'s receive path (empty owner = declined).
    void on_receive_gate(plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> mint)
    {
        m_receive_mint = std::move(mint);
    }

    // Unset = the generic default (same_host && any hint bit set).
    void on_policy(plexus::detail::move_only_function<bool(bool, dispatch_hint)> policy)
    {
        m_policy = std::move(policy);
    }

    void set_topic_hint(std::string_view fqn, dispatch_hint hint)
    {
        detail::upgrade_hints::note(*this, fqn, hint);
    }

    // An up-edge offers the peer a lane or awaits its publisher's offer, by role; the down-edge drops the peer's hold.
    void on_edge(std::string_view node_name, std::string_view fqn, demand_transition dir, demand_role role)
    {
        detail::upgrade_offers::edge(*this, node_name, fqn, dir, role);
    }

    // The node's publisher count for the fqn crossed zero; a gained one offers its lane to the peers already answered.
    void on_local_publisher(std::string_view fqn, bool present)
    {
        detail::upgrade_offers::publish(*this, fqn, present);
    }

    void on_companion(std::string_view node_name, std::string_view fqn, companion_event event, reader_claim reader = k_no_reader)
    {
        detail::upgrade_lanes::hear(*this, node_name, fqn, event, reader);
    }

    // A medium with no placement verb switches its holders without placing them.
    bool place(std::string_view fqn, reader_claim reader)
    {
        detail::send_lane<Channel> *lane = detail::find_send_lane(m_send, fqn);
        return lane != nullptr && (!lane->place || detail::place_claim(lane->holders, lane->place, reader));
    }

    // A publisher's marker resumes the receive lane it names from the first sequence it routes over it.
    void on_switch(std::string_view node_name, std::string_view fqn, std::uint64_t first_sequence)
    {
        detail::resume_receive_lane(m_receive, node_name, fqn, first_sequence);
    }

    // The subscriber's ready (naming its reader slot) and withdrawn answers and late offers leave here; unset, none do.
    void on_companion_send(plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event, reader_claim)> send)
    {
        m_companion_send_cb = std::move(send);
    }

    // Runs with the channel of a lane about to be destroyed, on every path that destroys one.
    void on_lane_released(plexus::detail::move_only_function<void(Channel &)> released)
    {
        m_lane_released_cb = std::move(released);
    }

    // What the subscribe response tells the peer about the lane: offered, not_offered (withheld) or no_publisher.
    companion_event offer_for(std::string_view node_name, std::string_view fqn) const
    {
        return detail::upgrade_offers::offer_for(*this, node_name, fqn);
    }

    // The lane once the peer answered ready, and whether this message fits it; no lane keeps the pair on the wire.
    lane_route<Channel> companion_for(std::string_view node_name, std::string_view fqn, std::size_t bytes)
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
        detail::upgrade_offers::forget_peer(*this, node_name);
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
    detail::offer_table m_offers;
    detail::receive_lanes m_receive;
    detail::send_lanes<Channel> m_send;
    detail::upgrade_hint_table m_hints;
    plexus::detail::move_only_function<upgrade_mint<Channel>(std::string_view)> m_mint;
    plexus::detail::move_only_function<upgrade_receive(std::string_view, std::string_view)> m_receive_mint;
    plexus::detail::move_only_function<bool(bool, dispatch_hint)> m_policy;
    plexus::detail::move_only_function<void(std::chrono::milliseconds)> m_schedule_retry_cb;
    plexus::detail::move_only_function<void(Channel &)> m_lane_released_cb;
    plexus::detail::move_only_function<void(std::string_view, std::string_view, companion_event, reader_claim)> m_companion_send_cb;

    friend struct detail::upgrade_hints;
    friend struct detail::upgrade_lanes;
    friend struct detail::upgrade_offers;
    friend struct detail::upgrade_send_lanes;
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RETRY_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_RETRY_H

#include "plexus/io/remembered_demand.h"
#include "plexus/io/subscribe_retry_config.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/peer_session_gate.h"
#include "plexus/io/detail/forwarder_subscribe.h"
#include "plexus/io/detail/peer_session_complete.h"

#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"

#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <system_error>

namespace plexus::io::detail {

struct unacked_subscribe
{
    std::uint8_t attempts;
    std::uint64_t topic_hash;
};

struct subscribe_retry_state
{
    bool armed{false};
    bool redeclared{false};
    std::vector<unacked_subscribe> unacked;
    std::optional<subscribe_retry_config> config;
};

inline bool retries(const subscribe_retry_state &r) noexcept
{
    return r.config && r.config->max_attempts > 0;
}

inline void reset_retry_cycle(subscribe_retry_state &r) noexcept
{
    r.unacked.clear();
    r.armed      = false;
    r.redeclared = false;
}

inline bool take_unacked(subscribe_retry_state &r, std::uint64_t topic_hash)
{
    const auto it = std::ranges::find(r.unacked, topic_hash, &unacked_subscribe::topic_hash);
    if(it == r.unacked.end())
        return false;
    r.unacked.erase(it);
    return true;
}

// The topics re-send together, so one wait serves them all: the one for the topic that has re-sent least.
inline std::chrono::milliseconds next_retry_wait(const subscribe_retry_state &r)
{
    const std::uint8_t fewest      = std::ranges::min(r.unacked, {}, &unacked_subscribe::attempts).attempts;
    std::chrono::milliseconds wait = r.config->first_interval;
    for(std::uint8_t k = 0; k < fewest && wait < r.config->max_interval; ++k)
        wait *= 2;
    return std::min(wait, r.config->max_interval);
}

template<typename Forwarder>
const remembered_demand *demand_for(const Forwarder &messages, const std::string &node_name, std::uint64_t topic_hash)
{
    for(const auto &d : messages.remembered_topics(node_name))
        if(wire::fqn_topic_hash(d.fqn) == topic_hash)
            return &d;
    return nullptr;
}

template<typename Session>
void release_subscribe(Session &s)
{
    if(s.m_outstanding_subscribes != 0)
        --s.m_outstanding_subscribes;
    s.maybe_fire_ready();
}

template<typename Session>
void surface_subscribe_outcome(Session &s, const wire::subscribe_response &resp);

template<typename Session>
void exhaust_subscribe(Session &s, const unacked_subscribe &entry)
{
    detail::report_drop(s, drop_cause::unacknowledged, entry.topic_hash);
    s.m_logger.warn("plexus: peer_session subscribe_unacknowledged");
    detail::surface_subscribe_outcome(s, wire::subscribe_response{.topic_hash = entry.topic_hash, .status = wire::subscribe_status::unacknowledged});
    release_subscribe(s);
}

// A topic unsubscribed meanwhile has no demand left to re-send and settles quietly.
template<typename Session>
void settle_subscribe(Session &s, const unacked_subscribe &entry)
{
    if(demand_for(s.m_messages, s.m_ctx.node_name, entry.topic_hash) == nullptr)
        return release_subscribe(s);
    exhaust_subscribe(s, entry);
}

// The re-send bypasses attach(): its subscribe gate holds demand as a set, so a second attach emits nothing.
template<typename Session>
void resend_subscribe(Session &s, unacked_subscribe &entry)
{
    if(const remembered_demand *d = demand_for(s.m_messages, s.m_ctx.node_name, entry.topic_hash))
    {
        detail::send_subscribe(s.m_messages, s.m_channel, d->fqn, entry.topic_hash, d->type_id, d->qos, d->type_name);
        ++entry.attempts;
    }
}

template<typename Session>
bool is_settled(const Session &s, const unacked_subscribe &entry)
{
    return entry.attempts >= s.m_retry.config->max_attempts || demand_for(s.m_messages, s.m_ctx.node_name, entry.topic_hash) == nullptr;
}

template<typename Session>
void arm_subscribe_retry(Session &s);

// Settling runs user callbacks, which may subscribe again or tear the session down, so it works on a copy.
template<typename Session>
void on_subscribe_retry_due(Session &s)
{
    std::vector<unacked_subscribe> kept;
    std::vector<unacked_subscribe> settled;
    for(const auto &u : s.m_retry.unacked)
        (is_settled(s, u) ? settled : kept).push_back(u);
    s.m_retry.unacked = std::move(kept);
    for(auto &u : s.m_retry.unacked)
        resend_subscribe(s, u);
    for(const auto &u : settled)
        if(!s.m_torn_down)
            settle_subscribe(s, u);
    arm_subscribe_retry(s);
}

// The handshake owns the session timer until completion; the retry owns it after.
template<typename Session>
void arm_subscribe_retry(Session &s)
{
    if(s.m_retry.armed || s.m_retry.unacked.empty() || !s.is_complete())
        return;
    s.m_retry.armed = true;
    s.m_session_timer.expires_after(next_retry_wait(s.m_retry));
    s.m_waits.begin();
    s.m_session_timer.async_wait(
            [&s](std::error_code ec)
            {
                s.m_waits.end();
                if(ec || s.m_torn_down)
                    return;
                s.m_retry.armed = false;
                on_subscribe_retry_due(s);
            });
}

template<typename Session>
void note_subscribe_sent(Session &s, std::uint64_t topic_hash)
{
    if(!retries(s.m_retry))
        return;
    s.m_retry.unacked.push_back({.attempts = 0, .topic_hash = topic_hash});
    arm_subscribe_retry(s);
}

// The acceptor's own completion-time declarations can overtake its handshake response and be dropped by a peer
// that is not yet complete; the first established-only frame from the peer proves it is, so they go once more.
template<typename Session>
void redeclare_once(Session &s)
{
    if(!retries(s.m_retry) || s.m_retry.redeclared)
        return;
    s.m_retry.redeclared = true;
    detail::redeclare_all(s);
}

}

#endif

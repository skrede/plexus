#ifndef HPP_GUARD_PLEXUS_IO_PEER_LIVELINESS_H
#define HPP_GUARD_PLEXUS_IO_PEER_LIVELINESS_H

#include "plexus/io/liveliness_options.h"
#include "plexus/io/peer_liveliness_event.h"
#include "plexus/io/liveliness_peer_storage.h"

#include "plexus/io/detail/liveliness_fuse.h"

#include "plexus/node_id.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::io {

// The clock-free peer-liveliness arbiter: every verb that needs time takes a monotonic now_ns
// supplied by the caller, so the fusion can never desync from the engine's one Clock. It fuses the
// three per-peer signals (awareness, heartbeat freshness, session presence) into one transition-
// latched alive/lost verdict, assembling the latch always but emitting on_verdict only while a
// subscriber is present. Transport drop dominates: a drop invalidates any pre-drop heartbeat and
// settles the verdict the same turn.
template<typename Storage = std_map_liveliness_peer_storage>
class peer_liveliness
{
public:
    explicit peer_liveliness(const liveliness_options &opts)
            : m_miss_window_ns(miss_window(opts))
            , m_policy(opts.policy)
            , m_subscribers(0)
    {
    }

    void note_heartbeat(const node_id &id, std::uint64_t now_ns)
    {
        if(m_policy == combine::session_authoritative)
            return;
        m_storage.at_or_insert(id).last_heartbeat_ns = now_ns;
    }

    void note_awareness(const node_id &id, std::uint64_t)
    {
        if(m_policy == combine::session_authoritative)
            return;
        m_storage.at_or_insert(id).aware = true;
    }

    void note_awareness_lost(const node_id &id)
    {
        if(peer_state *state = m_storage.find(id))
            state->aware = false;
    }

    void note_session_up(const node_id &id)
    {
        m_storage.at_or_insert(id).session = session_presence::up;
    }

    void note_session_down(const node_id &id, std::uint64_t now_ns)
    {
        peer_state &state = m_storage.at_or_insert(id);
        state.session       = session_presence::down;
        state.dropped_at_ns = now_ns;
        settle(id, state, now_ns);
    }

    void evaluate(std::uint64_t now_ns)
    {
        m_storage.for_each([&](const node_id &id, peer_state &state) { settle(id, state, now_ns); });
        m_storage.erase_if([this, now_ns](const peer_state &state) { return is_reapable(state, now_ns); });
    }

    void on_verdict(plexus::detail::move_only_function<void(const peer_liveliness_event &)> cb)
    {
        m_on_verdict = std::move(cb);
    }

    void add_subscriber()
    {
        ++m_subscribers;
    }

    void remove_subscriber()
    {
        if(m_subscribers > 0)
            --m_subscribers;
    }

    std::size_t subscribers() const
    {
        return m_subscribers;
    }

    template<typename Fn>
    void for_each_peer(Fn fn)
    {
        m_storage.for_each(std::move(fn));
    }

private:
    static std::uint64_t miss_window(const liveliness_options &opts)
    {
        const std::uint64_t interval = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::nanoseconds>(opts.heartbeat_interval).count());
        return static_cast<std::uint64_t>(opts.heartbeat_miss_limit) * interval;
    }

    // hb_seen requires a stamp strictly newer than the last drop (the staleness guard); hb_fresh
    // adds the N-miss window, so a heartbeat reads dead after miss_limit silent intervals.
    detail::signal_view derive(const peer_state &state, std::uint64_t now_ns) const
    {
        const bool hb_seen  = state.last_heartbeat_ns != 0 && state.last_heartbeat_ns > state.dropped_at_ns;
        const bool hb_fresh = hb_seen && now_ns - state.last_heartbeat_ns <= m_miss_window_ns;
        return detail::signal_view{state.session == session_presence::up,
                                   state.session != session_presence::absent,
                                   state.aware,
                                   hb_fresh,
                                   hb_seen};
    }

    void settle(const node_id &id, peer_state &state, std::uint64_t now_ns)
    {
        const detail::signal_view sv       = derive(state, now_ns);
        const detail::fuse_outcome outcome = detail::fuse_signals(m_policy, sv);
        if(outcome == detail::fuse_outcome::no_verdict)
            return;
        const bool alive = outcome == detail::fuse_outcome::alive;
        if(state.verdict_seen && alive == state.alive_latched)
            return;
        state.alive_latched = alive;
        state.verdict_seen  = true;
        emit(id, sv, outcome);
    }

    void emit(const node_id &id, const detail::signal_view &sv, detail::fuse_outcome outcome)
    {
        if(m_subscribers == 0 || !m_on_verdict)
            return;
        const liveliness_verdict verdict =
                outcome == detail::fuse_outcome::alive ? liveliness_verdict::alive : liveliness_verdict::lost;
        m_on_verdict(peer_liveliness_event{id, verdict, detail::contributing_mask(sv, outcome)});
    }

    bool is_reapable(const peer_state &state, std::uint64_t now_ns) const
    {
        const detail::signal_view sv = derive(state, now_ns);
        return state.verdict_seen && !state.alive_latched && !sv.session_up && !sv.aware && !sv.hb_fresh;
    }

    std::uint64_t m_miss_window_ns;
    combine m_policy;
    std::size_t m_subscribers;
    Storage m_storage;
    plexus::detail::move_only_function<void(const peer_liveliness_event &)> m_on_verdict;
};

}

#endif

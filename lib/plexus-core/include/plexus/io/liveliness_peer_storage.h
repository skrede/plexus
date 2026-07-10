#ifndef HPP_GUARD_PLEXUS_IO_LIVELINESS_PEER_STORAGE_H
#define HPP_GUARD_PLEXUS_IO_LIVELINESS_PEER_STORAGE_H

#include "plexus/io/detail/fixed_flat_map.h"

#include "plexus/node_id.h"

#include <map>
#include <cstddef>
#include <cstdint>
#include <iterator>

namespace plexus::io {

enum class session_presence : std::uint8_t
{
    absent,
    up,
    down
};

// The per-peer arbiter state. dropped_at_ns stamps the last transport drop so a heartbeat older
// than it cannot vote (the post-drop staleness guard). alive_latched is the last emitted verdict
// (the anti-flap latch); verdict_seen separates the first verdict from a latched repeat. A pure
// aggregate: value-initialization zeroes every field (session absent, both flags false).
struct peer_state
{
    std::uint64_t last_heartbeat_ns;
    std::uint64_t dropped_at_ns;
    session_presence session;
    bool aware;
    bool alive_latched;
    bool verdict_seen;
};

// The default (PC) arbiter backend: an unbounded heap-backed std::map keyed by node_id. The
// constrained-target build swaps in fixed_liveliness_peer_storage<N> via the arbiter's Storage
// template param; both expose the same at_or_insert/find/erase/for_each/erase_if surface.
class std_map_liveliness_peer_storage
{
public:
    peer_state &at_or_insert(const node_id &id)
    {
        return m_table[id];
    }

    peer_state *find(const node_id &id)
    {
        auto it = m_table.find(id);
        return it == m_table.end() ? nullptr : &it->second;
    }

    void erase(const node_id &id)
    {
        m_table.erase(id);
    }

    template<typename Fn>
    void for_each(Fn fn)
    {
        for(auto &[id, state] : m_table)
            fn(id, state);
    }

    template<typename Pred>
    void erase_if(Pred pred)
    {
        for(auto it = m_table.begin(); it != m_table.end();)
            it = pred(it->second) ? m_table.erase(it) : std::next(it);
    }

private:
    std::map<node_id, peer_state> m_table;
};

// The constrained-target (MCU) arbiter backend: the same verb surface over one fixed_flat_map.
// The (N+1)-th distinct peer key fails closed — the defined refusal fixed_flat_map establishes.
template<std::size_t N>
class fixed_liveliness_peer_storage
{
public:
    peer_state &at_or_insert(const node_id &id)
    {
        return m_table.at_or_insert(id);
    }

    peer_state *find(const node_id &id)
    {
        return m_table.find(id);
    }

    void erase(const node_id &id)
    {
        m_table.erase(id);
    }

    template<typename Fn>
    void for_each(Fn fn)
    {
        m_table.for_each(fn);
    }

    template<typename Pred>
    void erase_if(Pred pred)
    {
        m_table.for_each([&](const node_id &id, peer_state &state) {
            if(pred(state))
                m_table.erase(id);
        });
    }

private:
    detail::fixed_flat_map<node_id, peer_state, N> m_table;
};

}

#endif

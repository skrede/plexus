#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_DISCOVERY_FLOOD_CAP_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_DISCOVERY_FLOOD_CAP_H

#include "plexus/native/discovery_options.h"

#include <chrono>
#include <string>
#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::native::detail {

// The admission gate for the open multicast group, keyed by the datagram's kernel source host
// string. admit() bounds the distinct admitted-source set (max_peers) and one source's announce
// rate (per_source within a window), mirroring the data-plane demux precedent
// (lib/plexus-datagram/.../inbound_demux.h): an already-admitted source is never refused by the
// cap, so a flood of new sources cannot evict an established peer (evict_lru off, the default).
// Templated on Clock so a virtual clock drives the rate window deterministically in tests.
template<typename Clock = std::chrono::steady_clock>
class discovery_flood_cap
{
    using time_point = typename Clock::time_point;

    struct source_state
    {
        time_point last_seen;
        time_point window_start;
        std::size_t window_count;
    };

public:
    explicit discovery_flood_cap(flood_cap_options options)
            : m_options(std::move(options))
    {
    }

    bool admit(const std::string &source, time_point now)
    {
        auto it = m_peers.find(source);
        if(it != m_peers.end())
            return refresh(it->second, now);
        return admit_new(source, now);
    }

    std::size_t size() const noexcept
    {
        return m_peers.size();
    }

private:
    bool rate_limited() const noexcept
    {
        return m_options.per_source_max != 0;
    }

    bool capped() const noexcept
    {
        return m_options.max_peers != 0;
    }

    // An established source refreshes its window then passes the cap unconditionally.
    bool refresh(source_state &state, time_point now)
    {
        if(!rate_window_allows(state, now))
            return false;
        state.last_seen = now;
        return true;
    }

    bool admit_new(const std::string &source, time_point now)
    {
        if(capped() && m_peers.size() >= m_options.max_peers)
        {
            if(!m_options.evict_lru)
                return false;
            evict_oldest();
        }
        m_peers.emplace(source, source_state{now, now, 1});
        return true;
    }

    bool rate_window_allows(source_state &state, time_point now)
    {
        if(!rate_limited())
            return true;
        if(now - state.window_start >= m_options.per_source_window)
        {
            state.window_start = now;
            state.window_count = 0;
        }
        if(state.window_count >= m_options.per_source_max)
            return false;
        ++state.window_count;
        return true;
    }

    void evict_oldest()
    {
        auto oldest = m_peers.begin();
        for(auto it = m_peers.begin(); it != m_peers.end(); ++it)
            if(it->second.last_seen < oldest->second.last_seen)
                oldest = it;
        m_peers.erase(oldest);
    }

    flood_cap_options m_options;
    std::unordered_map<std::string, source_state> m_peers;
};

}

#endif

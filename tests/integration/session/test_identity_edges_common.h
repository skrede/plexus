#ifndef HPP_GUARD_TESTS_INTEGRATION_IDENTITY_EDGES_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_IDENTITY_EDGES_COMMON_H

#include "test_crossed_dial_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/node_name.h"
#include "plexus/io/peer_kind.h"

#include <vector>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace identity_edges_fixture {

using namespace crossed_dial_fixture;

enum class edge : std::uint8_t
{
    connected,
    reconnected,
    ready,
    disconnected
};

class identity_log final : public plexus::io::observer
{
    struct noted
    {
        node_id id;
        edge kind;
    };

public:
    identity_log()
            : m_noted()
    {
    }

    void on_peer_connected(const node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        m_noted.push_back({id, edge::connected});
    }

    void on_peer_reconnected(const node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        m_noted.push_back({id, edge::reconnected});
    }

    void on_peer_ready(const node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        m_noted.push_back({id, edge::ready});
    }

    void on_peer_disconnected(const node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        m_noted.push_back({id, edge::disconnected});
    }

    std::vector<edge> of(const node_id &id) const
    {
        std::vector<edge> out;
        for(const noted &n : m_noted)
            if(n.id == id)
                out.push_back(n.kind);
        return out;
    }

    std::vector<node_id> ids() const
    {
        std::vector<node_id> out;
        for(const noted &n : m_noted)
            if(std::ranges::find(out, n.id) == out.end())
                out.push_back(n.id);
        return out;
    }

private:
    std::vector<noted> m_noted;
};

inline std::int32_t ups(const std::vector<edge> &edges)
{
    return static_cast<std::int32_t>(std::ranges::count_if(edges, [](edge k) { return k == edge::connected || k == edge::reconnected; }));
}

inline std::int32_t downs(const std::vector<edge> &edges)
{
    return static_cast<std::int32_t>(std::ranges::count(edges, edge::disconnected));
}

inline bool carried(engine &e, const node_id &id)
{
    bool found = false;
    e.registry().for_each_connected([&](const node_id &, engine::session_type &s) { found = found || s.msg_peer().node_name == plexus::io::node_name_of(id); });
    return found;
}

// An up opens a period and a down closes it; a ready belongs inside a period, once.
inline bool balanced(const identity_log &log, engine &e, const node_id &id)
{
    bool up    = false;
    bool ready = false;
    for(const edge k : log.of(id))
    {
        if(k == edge::ready)
        {
            if(!up || ready)
                return false;
            ready = true;
            continue;
        }
        const bool opens = k != edge::disconnected;
        if(opens == up)
            return false;
        up    = opens;
        ready = false;
    }
    return up == carried(e, id);
}

inline bool balanced(const identity_log &log, engine &e)
{
    const std::vector<node_id> seen = log.ids();
    return std::ranges::all_of(seen, [&](const node_id &id) { return balanced(log, e, id); });
}

}

#endif

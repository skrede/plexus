#ifndef HPP_GUARD_TESTS_INTEGRATION_IDLE_DIALED_SLOT_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_IDLE_DIALED_SLOT_COMMON_H

#include "test_reach_guard_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/node_id.h"

#include <vector>
#include <string_view>

namespace idle_slot_fixture {

using namespace reach_guard_fixture;

enum class edge
{
    connected,
    disconnected,
    reconnected,
    dead
};

class edge_log final : public plexus::io::observer
{
public:
    explicit edge_log(const plexus::node_id &peer)
            : m_peer(peer)
            , m_edges()
    {
    }

    void on_peer_connected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        note(id, edge::connected);
    }

    void on_peer_disconnected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        note(id, edge::disconnected);
    }

    void on_peer_reconnected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        note(id, edge::reconnected);
    }

    void on_peer_dead(const plexus::node_id &id, std::string_view, plexus::io::peer_kind) override
    {
        note(id, edge::dead);
    }

    const std::vector<edge> &edges() const noexcept
    {
        return m_edges;
    }

private:
    plexus::node_id m_peer;
    std::vector<edge> m_edges;

    void note(const plexus::node_id &id, edge e)
    {
        if(id == m_peer)
            m_edges.push_back(e);
    }
};

struct idle_pair : guarded_pair
{
    edge_log log;

    explicit idle_pair(const reconnect_config &redial = forever_cfg())
            : guarded_pair(redial)
            , log(id_b)
    {
        a.add_observer(log);
    }

    void connect()
    {
        a.note_peer(id_b, ep_b);
        a.reach(id_b);
        ex.drain();
    }

    void drop_from_b()
    {
        b.session_for(id_a)->tear_down();
        ex.drain();
    }

    void reach_from_b()
    {
        b.note_peer(id_a, ep_a);
        b.reach(id_a);
        ex.drain();
    }
};

}

#endif

#ifndef HPP_GUARD_PLEXUS_GRAPH_VECTOR_GRAPH_CHANGE_LOG_H
#define HPP_GUARD_PLEXUS_GRAPH_VECTOR_GRAPH_CHANGE_LOG_H

#include "plexus/graph/graph_change.h"

#include "plexus/node_id.h"

#include <span>
#include <vector>

namespace plexus::graph
{

// The host edge-log: appended {who, kind} deltas that an observes_graph() observer drains as a span
// after each coalesced wakeup, then clears. It grows once like the other heap-backed tables and is
// maintained only while an observer declares interest. The bounded profile substitutes
// null_graph_change_log, whose identical append/drain/clear surface holds nothing.
class vector_graph_change_log
{
public:
    void append(const plexus::node_id &who, change_kind kind)
    {
        m_log.push_back(graph_change{who, kind});
    }

    void append(const graph_change &change)
    {
        m_log.push_back(change);
    }

    std::span<const graph_change> drain() const noexcept
    {
        return std::span<const graph_change>{m_log};
    }

    void clear() noexcept
    {
        m_log.clear();
    }

private:
    std::vector<graph_change> m_log;
};

}

#endif

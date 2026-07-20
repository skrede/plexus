#ifndef HPP_GUARD_PLEXUS_GRAPH_NULL_GRAPH_CHANGE_LOG_H
#define HPP_GUARD_PLEXUS_GRAPH_NULL_GRAPH_CHANGE_LOG_H

#include "plexus/graph/graph_change.h"

#include "plexus/node_id.h"

#include <span>

namespace plexus::graph
{

// The bounded-profile twin: a members-less struct whose append is a no-op and whose drain is always
// empty, so bounded<> pays zero for the edge-log by construction. It mirrors
// vector_graph_change_log's append/drain/clear surface exactly, so one template parameter threads
// both twins with no platform branch.
struct null_graph_change_log
{
    void append(const plexus::node_id &, change_kind) noexcept
    {
    }

    void append(const graph_change &) noexcept
    {
    }

    std::span<const graph_change> drain() const noexcept
    {
        return std::span<const graph_change>{};
    }

    void clear() noexcept
    {
    }
};

}

#endif

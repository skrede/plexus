#ifndef HPP_GUARD_PLEXUS_GRAPH_GRAPH_CHANGE_H
#define HPP_GUARD_PLEXUS_GRAPH_GRAPH_CHANGE_H

#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::graph
{

enum class change_kind : std::uint8_t
{
    appeared,
    disappeared,
    // A reported origin's only path went dead (its relay's session died) yet the identity is retained,
    // marked unreachable — a state distinct from disappeared (which means the participant is gone). The
    // reachable kind is its recovery counterpart, fired when the path returns.
    unreachable,
    reachable
};

// The minimal graph delta: which participant crossed the graph and in which direction, owned by
// value so the record outlives the transport buffer that decoded it. It is deliberately not a full
// participant record — a consumer that needs types, counts, or reachability re-snapshots
// participants()/topics(); anything richer here slides toward a typed subscription.
struct graph_change
{
    plexus::node_id who;
    change_kind kind;
};

}

#endif

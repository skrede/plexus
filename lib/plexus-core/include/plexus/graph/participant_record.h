#ifndef HPP_GUARD_PLEXUS_GRAPH_PARTICIPANT_RECORD_H
#define HPP_GUARD_PLEXUS_GRAPH_PARTICIPANT_RECORD_H

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::graph
{

struct route
{
    io::endpoint transport;
    // Relay hop for a transitively-observed peer; std::nullopt is the reserved
    // direct-only state, never a zero-node_id sentinel.
    std::optional<plexus::node_id> via;
};

enum class observation : std::uint8_t
{
    directly_observed,
    reported
};

struct provenance
{
    observation how;
    // The peer that reported this entry; std::nullopt is the reserved direct-only
    // state, never a zero-node_id sentinel.
    std::optional<plexus::node_id> reporter;
};

struct participant_record
{
    plexus::node_id id;
    route reach;
    provenance origin;
};

struct snapshot_result
{
    std::size_t count;
    bool truncated;
};

}

#endif

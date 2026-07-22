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

// Whether a candidate's path is currently usable. A reported origin whose relay's session dies
// degrades to unreachable WITHOUT losing its identity or its via edge, so it stays distinguishable
// from a peer that genuinely left (which retires the row outright). A directly-observed peer is
// always reachable; the field defaults so a direct-only record is unperturbed.
enum class reachability : std::uint8_t
{
    reachable,
    unreachable
};

struct provenance
{
    observation how;
    // The peer that reported this entry; std::nullopt is the reserved direct-only
    // state, never a zero-node_id sentinel.
    std::optional<plexus::node_id> reporter;
    reachability reach_status{reachability::reachable};
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

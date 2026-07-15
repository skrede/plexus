#ifndef HPP_GUARD_PLEXUS_GRAPH_TOPIC_RECORD_H
#define HPP_GUARD_PLEXUS_GRAPH_TOPIC_RECORD_H

#include "plexus/node_id.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::graph
{

// The per-topic distinct-type bound. An owning backend stores each name at its wire lid
// (wire::detail::k_max_type_name, 512 bytes), so four names cost 2048 bytes of name storage per
// topic. The common case is one type per topic; a publisher disagreement past four distinct types
// is refused and flagged, never grown unbounded. Interim: an on-target footprint measurement is
// what moves this value.
constexpr std::size_t k_topic_type_list_cap = 4;

// Which side of a topic a participant sits on. The edge direction is the record's own field, so a
// publisher or subscriber count is a reduction over the records, never a maintained index.
enum class topic_role : std::uint8_t
{
    publisher,
    subscriber
};

// The distinct opaque type names a topic's participants declared, told apart by the numeric type
// id each carried. A count of zero is the reserved undeclared state, distinct from a single empty
// name (a participant that declared an empty type). A count above one IS the polytype conflict
// marker: the disagreement is surfaced, never settled by a silent first-wins.
struct type_name_list
{
    std::array<std::string_view, k_topic_type_list_cap> names;
    std::size_t count;
};

// One (participant, topic, role) edge of the graph. The views borrow the table's owned copies and
// stay valid only for the sweep that produced the record.
struct topic_record
{
    plexus::node_id node;
    std::string_view name;
    type_name_list types;
    topic_role role;
    // The type list is not the whole truth for this topic: a name was clipped to its bound, or a
    // distinct type past k_topic_type_list_cap was refused.
    bool truncated;
};

}

#endif

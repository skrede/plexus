#ifndef HPP_GUARD_PLEXUS_DETAIL_TOPIC_SWEEP_H
#define HPP_GUARD_PLEXUS_DETAIL_TOPIC_SWEEP_H

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/match/key_pattern.h"

#include <span>
#include <cstddef>
#include <optional>
#include <string_view>

namespace plexus::detail {

// A topic name past the pattern's own length bound belongs to no pattern's keyset, so it fails a
// filtered query rather than aborting the sweep. An absent filter admits every topic.
inline bool topic_in(std::string_view topic, const std::optional<match::key_pattern> &filter)
{
    if(!filter)
        return true;
    const auto key = match::key_pattern::make(topic);
    return key.has_value() && filter->intersects(*key);
}

// Fill out to capacity with the edges the predicate keeps, reporting overflow as a count plus a
// flag — never abort, never evict (reject-and-count at the span boundary). No lock, no allocation:
// the records borrow the table's own storage for the caller's turn.
template<typename Table, typename Pred>
graph::snapshot_result sweep_topics(const Table &table, std::span<graph::topic_record> out, Pred keep)
{
    std::size_t filled    = 0;
    bool        truncated = false;
    table.for_each([&](const graph::topic_record &rec) {
        if(!keep(rec))
            return;
        if(filled == out.size())
        {
            truncated = true;
            return;
        }
        out[filled++] = rec;
    });
    return graph::snapshot_result{filled, truncated};
}

// The table yields one record per (participant, topic, role) edge and holds at most one edge per
// participant per role, so counting the matching edges counts the distinct participants on that
// side — the reduction, not a counter kept beside the table.
template<typename Table>
std::size_t count_topic_role(const Table &table, std::string_view topic, graph::topic_role role)
{
    std::size_t found = 0;
    table.for_each([&](const graph::topic_record &rec) {
        if(rec.name == topic && rec.role == role)
            ++found;
    });
    return found;
}

}

#endif

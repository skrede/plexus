#ifndef HPP_GUARD_PLEXUS_GRAPH_TOPIC_TYPE_TABLE_H
#define HPP_GUARD_PLEXUS_GRAPH_TOPIC_TYPE_TABLE_H

#include "plexus/graph/topic_record.h"
#include "plexus/graph/std_map_topic_storage.h"

#include "plexus/node_id.h"

#include "plexus/wire/subscribe.h"

#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus::graph
{

// The owning graph table: what every participant declared about every topic, folded into one
// enumerable set of (participant, topic, role) edges. It COPIES every name handed to it — the fold
// runs on a decoded frame whose buffer the transport recycles the moment it returns.
//
// Overflow is reject-and-count, never an abort and never an eviction: an edge with no room is
// dropped and tallied, a distinct type past the cap flags its topic truncated and is tallied. A
// full table therefore keeps telling the truth about the peers it already knows.
template<typename Storage = std_map_topic_storage>
class basic_topic_type_table
{
public:
    basic_topic_type_table()
        : m_storage{}
        , m_dropped{0}
        , m_truncations{0}
    {
    }

    upsert_result upsert(const topic_edge &edge)
    {
        if(edge.topic.size() > wire::detail::k_max_fqn)
            return drop();
        const bool clipped = edge.type_id.has_value() && edge.type_name.size() > wire::detail::k_max_type_name;
        topic_edge bounded = edge;
        bounded.type_name  = edge.type_name.substr(0, wire::detail::k_max_type_name);
        return tally(m_storage.upsert(bounded, clipped), clipped);
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        m_storage.for_each(std::move(fn));
    }

    // Topic knowledge is session-scoped: a peer's edges leave with its session. A topic left with
    // no edges goes whole, so a peer that churns cannot hold a slot it no longer occupies.
    bool remove_node(const plexus::node_id &node)
    {
        return m_storage.remove_node(node);
    }

    // The inverse of a single upsert: one participant leaves one side of one topic, the other side
    // and every other participant untouched. A topic left with no edges goes whole, as under
    // remove_node — the type names go with the entry, never orphaned behind a departed edge.
    bool remove_edge(const plexus::node_id &node, std::string_view topic, topic_role role)
    {
        return m_storage.remove_edge(node, topic, role);
    }

    // Edges refused for want of room. An over-long topic name is refused here too: clipping it
    // would alias two distinct topics onto one entry.
    std::size_t dropped() const
    {
        return m_dropped;
    }

    // Topics that lost detail: a type name clipped to its bound, or a distinct type past the cap.
    std::size_t truncations() const
    {
        return m_truncations;
    }

private:
    Storage m_storage;
    std::size_t m_dropped;
    std::size_t m_truncations;

    upsert_result drop()
    {
        ++m_dropped;
        return upsert_result{upsert_outcome::dropped, false};
    }

    upsert_result tally(upsert_result result, bool clipped)
    {
        if(result.outcome == upsert_outcome::dropped)
            return drop();
        if(result.outcome == upsert_outcome::truncated || clipped)
        {
            ++m_truncations;
            return upsert_result{upsert_outcome::truncated, result.changed};
        }
        return result;
    }
};

using topic_type_table = basic_topic_type_table<>;

}

#endif

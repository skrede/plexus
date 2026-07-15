#ifndef HPP_GUARD_PLEXUS_GRAPH_TOPIC_TYPE_TABLE_H
#define HPP_GUARD_PLEXUS_GRAPH_TOPIC_TYPE_TABLE_H

#include "plexus/graph/topic_record.h"

#include "plexus/node_id.h"

#include "plexus/wire/subscribe.h"

#include <map>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace plexus::graph
{

// The default (PC) topic backend: an unbounded heap-backed std::map keyed by an OWNED topic name.
// The constrained-target build swaps in fixed_topic_storage<N> through the Storage template param.
// Only the topic and participant counts go unbounded here: the per-topic type list keeps its cap
// on every backend, because that cap is the polytype conflict semantics and a flooding lid, not a
// memory tactic.
class std_map_topic_storage
{
    struct type_slot
    {
        std::string name;
        std::uint64_t id;
    };

    struct edge_slot
    {
        plexus::node_id node;
        topic_role role;
    };

    struct entry
    {
        std::vector<type_slot> types;
        std::vector<edge_slot> edges;
        bool truncated;
    };

public:
    upsert_outcome upsert(const topic_edge &edge, bool clipped)
    {
        entry &e    = at(edge.topic);
        e.truncated = e.truncated || clipped;
        note_edge(e, edge);
        return note_type(e, edge);
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const auto &[topic, e] : m_table)
            for(const edge_slot &edge : e.edges)
                fn(make_record(topic, e, edge));
    }

    void remove_node(const plexus::node_id &node)
    {
        for(auto &[topic, e] : m_table)
            std::erase_if(e.edges, [&](const edge_slot &s) { return s.node == node; });
        std::erase_if(m_table, [](const auto &kv) { return kv.second.edges.empty(); });
    }

private:
    std::map<std::string, entry, std::less<>> m_table;

    entry &at(std::string_view topic)
    {
        auto it = m_table.find(topic);
        if(it == m_table.end())
            it = m_table.emplace(topic, entry{}).first;
        return it->second;
    }

    static void note_edge(entry &e, const topic_edge &edge)
    {
        for(const edge_slot &slot : e.edges)
            if(slot.node == edge.node && slot.role == edge.role)
                return;
        e.edges.push_back(edge_slot{edge.node, edge.role});
    }

    // Distinctness is settled on the numeric id, never on a string compare: the id is what the
    // declaration carries to tell two types apart, the names are what enumeration displays.
    static upsert_outcome note_type(entry &e, const topic_edge &edge)
    {
        if(!edge.type_id)
            return upsert_outcome::stored;
        for(const type_slot &slot : e.types)
            if(slot.id == *edge.type_id)
                return upsert_outcome::stored;
        if(e.types.size() == k_topic_type_list_cap)
        {
            e.truncated = true;
            return upsert_outcome::truncated;
        }
        e.types.push_back(type_slot{std::string{edge.type_name}, *edge.type_id});
        return upsert_outcome::stored;
    }

    static topic_record make_record(std::string_view topic, const entry &e, const edge_slot &edge)
    {
        type_name_list types{};
        types.count = e.types.size();
        for(std::size_t i = 0; i < e.types.size(); ++i)
            types.names[i] = e.types[i].name;
        return topic_record{edge.node, topic, types, edge.role, e.truncated};
    }
};

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

    upsert_outcome upsert(const topic_edge &edge)
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
    void remove_node(const plexus::node_id &node)
    {
        m_storage.remove_node(node);
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

    upsert_outcome drop()
    {
        ++m_dropped;
        return upsert_outcome::dropped;
    }

    upsert_outcome tally(upsert_outcome outcome, bool clipped)
    {
        if(outcome == upsert_outcome::dropped)
            return drop();
        if(outcome == upsert_outcome::truncated || clipped)
        {
            ++m_truncations;
            return upsert_outcome::truncated;
        }
        return outcome;
    }
};

using topic_type_table = basic_topic_type_table<>;

}

#endif

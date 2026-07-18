#ifndef HPP_GUARD_PLEXUS_GRAPH_STD_MAP_TOPIC_STORAGE_H
#define HPP_GUARD_PLEXUS_GRAPH_STD_MAP_TOPIC_STORAGE_H

#include "plexus/graph/topic_record.h"

#include "plexus/node_id.h"

#include <map>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>

namespace plexus::graph
{

// The default (PC) topic backend: an unbounded heap-backed std::map keyed by an OWNED topic name.
// A bounded node profile substitutes fixed_topic_storage<N> through the Storage template param
// instead. Only the topic and participant counts go unbounded here: the per-topic type list keeps
// its cap on every backend, because that cap is the polytype conflict semantics and a flooding lid,
// not a memory tactic.
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
    upsert_result upsert(const topic_edge &edge, bool clipped)
    {
        entry &e             = at(edge.topic);
        e.truncated          = e.truncated || clipped;
        const bool new_edge  = note_edge(e, edge);
        const upsert_result t = note_type(e, edge);
        return upsert_result{t.outcome, new_edge || t.changed};
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const auto &[topic, e] : m_table)
            for(const edge_slot &edge : e.edges)
                fn(make_record(topic, e, edge));
    }

    bool remove_node(const plexus::node_id &node)
    {
        bool erased = false;
        for(auto &[topic, e] : m_table)
            erased = std::erase_if(e.edges, [&](const edge_slot &s) { return s.node == node; }) != 0 || erased;
        std::erase_if(m_table, [](const auto &kv) { return kv.second.edges.empty(); });
        return erased;
    }

    bool remove_edge(const plexus::node_id &node, std::string_view topic, topic_role role)
    {
        auto it = m_table.find(topic);
        if(it == m_table.end())
            return false;
        const bool erased = std::erase_if(it->second.edges, [&](const edge_slot &s) { return s.node == node && s.role == role; }) != 0;
        if(it->second.edges.empty())
            m_table.erase(it);
        return erased;
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

    static bool note_edge(entry &e, const topic_edge &edge)
    {
        for(const edge_slot &slot : e.edges)
            if(slot.node == edge.node && slot.role == edge.role)
                return false;
        e.edges.push_back(edge_slot{edge.node, edge.role});
        return true;
    }

    // Distinctness is settled on the numeric id, never on a string compare: the id is what the
    // declaration carries to tell two types apart, the names are what enumeration displays.
    static upsert_result note_type(entry &e, const topic_edge &edge)
    {
        if(!edge.type_id)
            return upsert_result{upsert_outcome::stored, false};
        for(const type_slot &slot : e.types)
            if(slot.id == *edge.type_id)
                return upsert_result{upsert_outcome::stored, false};
        if(e.types.size() == k_topic_type_list_cap)
        {
            e.truncated = true;
            return upsert_result{upsert_outcome::truncated, false};
        }
        e.types.push_back(type_slot{std::string{edge.type_name}, *edge.type_id});
        return upsert_result{upsert_outcome::stored, true};
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

}

#endif

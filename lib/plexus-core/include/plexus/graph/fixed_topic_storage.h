#ifndef HPP_GUARD_PLEXUS_GRAPH_FIXED_TOPIC_STORAGE_H
#define HPP_GUARD_PLEXUS_GRAPH_FIXED_TOPIC_STORAGE_H

#include "plexus/graph/topic_record.h"

#include "plexus/node_id.h"

#include "plexus/wire/subscribe.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace plexus::graph
{

// The constrained-target topic backend: a dep-free flat array of Topics entries, a linear scan
// over the topic names. Each entry owns its topic name and its distinct type names inline, at
// their wire lids — a decoded declare's name lives in a buffer the transport recycles, so a
// borrowed view would dangle. Per-entry cost: k_max_fqn (1024) for the name, k_topic_type_list_cap
// x k_max_type_name (2048) for the type names, Edges x 17 for the participants.
//
// Both bounds are reject-and-count, not fail-closed: the (Topics+1)-th topic and the (Edges+1)-th
// participant of a topic are refused and tallied — never written out of bounds, never granted room
// by evicting a live peer.
template<std::size_t Topics, std::size_t Edges = 8>
class fixed_topic_storage
{
    struct type_slot
    {
        std::array<char, wire::detail::k_max_type_name> name;
        std::size_t len;
        std::uint64_t id;
    };

    struct edge_slot
    {
        plexus::node_id node;
        topic_role role;
    };

    struct entry
    {
        std::array<char, wire::detail::k_max_fqn> topic;
        std::array<type_slot, k_topic_type_list_cap> types;
        std::array<edge_slot, Edges> edges;
        std::size_t topic_len;
        std::size_t type_count;
        std::size_t edge_count;
        bool truncated;
        bool occupied;
    };

public:
    fixed_topic_storage()
        : m_slots{}
    {
    }

    upsert_outcome upsert(const topic_edge &edge, bool clipped)
    {
        entry *slot = find(edge.topic);
        if(slot == nullptr)
            slot = claim(edge.topic);
        if(slot == nullptr)
            return upsert_outcome::dropped;
        slot->truncated = slot->truncated || clipped;
        if(!note_edge(*slot, edge))
            return upsert_outcome::dropped;
        return note_type(*slot, edge);
    }

    template<typename Fn>
    void for_each(Fn fn) const
    {
        for(const entry &e : m_slots)
        {
            if(!e.occupied)
                continue;
            for(std::size_t i = 0; i < e.edge_count; ++i)
                fn(make_record(e, e.edges[i]));
        }
    }

    void remove_node(const plexus::node_id &node)
    {
        for(entry &e : m_slots)
        {
            drop_edges(e, node);
            if(e.edge_count == 0)
                e = entry{};
        }
    }

private:
    std::array<entry, Topics> m_slots;

    template<std::size_t N>
    static std::string_view view(const std::array<char, N> &src, std::size_t len)
    {
        return std::string_view{src.data(), len};
    }

    template<std::size_t N>
    static std::size_t copy_bounded(std::array<char, N> &dst, std::string_view src)
    {
        const std::size_t len = std::min(src.size(), N);
        std::copy_n(src.begin(), len, dst.begin());
        return len;
    }

    entry *find(std::string_view topic)
    {
        for(entry &e : m_slots)
            if(e.occupied && view(e.topic, e.topic_len) == topic)
                return &e;
        return nullptr;
    }

    entry *claim(std::string_view topic)
    {
        for(entry &e : m_slots)
        {
            if(e.occupied)
                continue;
            e.occupied  = true;
            e.topic_len = copy_bounded(e.topic, topic);
            return &e;
        }
        return nullptr;
    }

    static void drop_edges(entry &e, const plexus::node_id &node)
    {
        std::size_t kept = 0;
        for(std::size_t i = 0; i < e.edge_count; ++i)
            if(e.edges[i].node != node)
                e.edges[kept++] = e.edges[i];
        e.edge_count = kept;
    }

    static bool note_edge(entry &e, const topic_edge &edge)
    {
        for(std::size_t i = 0; i < e.edge_count; ++i)
            if(e.edges[i].node == edge.node && e.edges[i].role == edge.role)
                return true;
        if(e.edge_count == Edges)
            return false;
        e.edges[e.edge_count++] = edge_slot{edge.node, edge.role};
        return true;
    }

    // Distinctness is settled on the numeric id, never on a string compare: the id is what the
    // declaration carries to tell two types apart, the names are what enumeration displays.
    static upsert_outcome note_type(entry &e, const topic_edge &edge)
    {
        if(!edge.type_id)
            return upsert_outcome::stored;
        for(std::size_t i = 0; i < e.type_count; ++i)
            if(e.types[i].id == *edge.type_id)
                return upsert_outcome::stored;
        if(e.type_count == k_topic_type_list_cap)
        {
            e.truncated = true;
            return upsert_outcome::truncated;
        }
        type_slot &slot = e.types[e.type_count++];
        slot.id         = *edge.type_id;
        slot.len        = copy_bounded(slot.name, edge.type_name);
        return upsert_outcome::stored;
    }

    static topic_record make_record(const entry &e, const edge_slot &edge)
    {
        type_name_list types{};
        types.count = e.type_count;
        for(std::size_t i = 0; i < e.type_count; ++i)
            types.names[i] = view(e.types[i].name, e.types[i].len);
        return topic_record{edge.node, view(e.topic, e.topic_len), types, edge.role, e.truncated};
    }
};

}

#endif

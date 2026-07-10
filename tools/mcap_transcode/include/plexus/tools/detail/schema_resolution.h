#ifndef HPP_GUARD_PLEXUS_TOOLS_DETAIL_SCHEMA_RESOLUTION_H
#define HPP_GUARD_PLEXUS_TOOLS_DETAIL_SCHEMA_RESOLUTION_H

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_projection.h"

#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <string_view>
#include <unordered_map>

namespace plexus::tools::detail {

namespace rec = plexus::io::recording;

// The schema to attach to a channel, or an empty encoding to attach none (schemaId 0 —
// the serializer-agnostic opaque path a viewer leaves undecoded).
struct channel_schema
{
    std::string_view name;
    std::string_view encoding;
    std::string_view data;
};

// The label a declared data type resolves to: how the recorded bytes are framed
// (message_encoding) and the schema to attach. Built once from the preamble per type_id.
// The transcode validates NOTHING about whether the bytes match — the preamble is the
// self-describing source, the honesty is the consumer's contract (serializer-agnostic).
struct declared_label
{
    std::string message_encoding;
    std::string schema_name;
    std::string schema_encoding;
    std::string schema_data;
};

// Join topic_hash -> type name from the preamble schema (keyed by type_id) and from
// the endpoint/qos_change records that carry both topic_hash and an optional type_id.
// At HEAD the preamble schema is empty and samples carry no type_id, so the endpoint
// fqn is the load-bearing name source; the schema table is consulted when populated.
inline std::unordered_map<std::uint64_t, std::string>
build_topic_names(const rec::stream_definitions          &defs,
                  const std::vector<rec::decoded_record> &records)
{
    std::unordered_map<std::uint64_t, std::string> by_type_id;
    for(const auto &entry : defs.schema)
        by_type_id.emplace(entry.type_id, entry.type_name);

    std::unordered_map<std::uint64_t, std::string> by_topic;
    for(const auto &r : records)
    {
        const bool is_decl = r.category == rec::record_category::endpoint ||
                             r.category == rec::record_category::qos_change;
        if(!is_decl)
            continue;
        std::string name;
        if(r.type_id)
        {
            const auto it = by_type_id.find(*r.type_id);
            if(it != by_type_id.end())
                name = it->second;
        }
        if(name.empty() && !r.fqn.empty())
            name = r.fqn;
        if(!name.empty())
            by_topic[r.topic_hash] = std::move(name);
    }
    return by_topic;
}

inline std::unordered_map<std::uint64_t, declared_label>
build_declared_labels(const rec::stream_definitions &defs)
{
    std::unordered_map<std::uint64_t, declared_label> by_type_id;
    for(const auto &e : defs.schema)
    {
        if(e.message_encoding.empty())
            continue;
        by_type_id.emplace(
            e.type_id,
            declared_label{e.message_encoding, e.schema_name, e.schema_encoding,
                           std::string{reinterpret_cast<const char *>(e.schema_data.data()),
                                       e.schema_data.size()}});
    }
    return by_type_id;
}

// The type_id -> schema_hint join the endpoint records carry (schema_hint rides the
// per-publisher endpoint record). A zero hint is the no-hint sentinel and never recorded.
inline std::unordered_map<std::uint64_t, std::uint64_t>
build_schema_hints(const std::vector<rec::decoded_record> &records)
{
    std::unordered_map<std::uint64_t, std::uint64_t> by_type_id;
    for(const auto &r : records)
        if(r.category == rec::record_category::endpoint && r.type_id && r.schema_hint)
            by_type_id[*r.type_id] = r.schema_hint;
    return by_type_id;
}

}

#endif

#ifndef HPP_GUARD_PLEXUS_TOOLS_MCAP_SCHEMA_H
#define HPP_GUARD_PLEXUS_TOOLS_MCAP_SCHEMA_H

#include "plexus/detail/compat.h"

#include <string>
#include <cstdint>
#include <utility>
#include <optional>
#include <unordered_map>

namespace plexus::tools {

// A generic, vocabulary-free descriptor for the schema decoration a channel carries: how its
// messages are framed (message_encoding) and the schema to attach (name/encoding/data). An empty
// schema_encoding leaves the channel schema-less (schemaId 0, the opaque path). The transcode
// interprets none of these bytes — a provider hands them in as consumer-supplied data.
struct mcap_schema
{
    std::string message_encoding;
    std::string schema_name;
    std::string schema_encoding;
    std::string schema_data;
};

// Resolves the schema for a declared type, keyed on its type_id — the consumer-supplied escape
// hatch, which wins over every other source. An empty provider decorates nothing.
using schema_provider = plexus::detail::move_only_function<std::optional<mcap_schema>(std::uint64_t type_id)>;

// Translates an opaque schema_hint (see plexus::type_identity) to a schema. The transcode reads
// the type_id -> schema_hint map from the stream and feeds each hint here, so the translator is
// where a hint vocabulary is named and the generic transcode is not. An empty translator
// decorates nothing.
using hint_translator = plexus::detail::move_only_function<std::optional<mcap_schema>(std::uint64_t schema_hint)>;

inline schema_provider provider_from_map(std::unordered_map<std::uint64_t, mcap_schema> by_type_id)
{
    return [table = std::move(by_type_id)](std::uint64_t type_id) -> std::optional<mcap_schema>
    {
        const auto it = table.find(type_id);
        return it == table.end() ? std::nullopt : std::optional<mcap_schema>{it->second};
    };
}

// The bundled hint-translator a transcode passes by default: it maps a neutral concept hint to
// a well-known schema whose data it carries. Both the concept vocabulary it reads and the schema
// data it emits live in one host-only backend translation unit, so this declaration names neither
// a vocabulary nor a vendor — the generic transcode stays free of both.
hint_translator well_known_schema_translator();

}

#endif

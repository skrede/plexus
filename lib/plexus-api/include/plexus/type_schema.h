#ifndef HPP_GUARD_PLEXUS_API_TYPE_SCHEMA_H
#define HPP_GUARD_PLEXUS_API_TYPE_SCHEMA_H

#include <span>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus {

// The host-side, consumer-declared self-description of one wire type: the offline-decode
// key a recorder writes into the stream preamble so a projector resolves a codec/schema for
// the raw bytes a sample carries. A plain designated-initializer value aggregate (no raw
// pointers, no owning members), the public sibling of recording_qos / type_identity. Core
// never interprets the fields — it lays them down verbatim and reads them back.
//
// The fields follow the required-vs-meaningful-absence discipline:
//   - type_id + message_encoding are required to decode a sample (the key the sample's
//     recorded type_id resolves against, and how its bytes are framed for the projector);
//   - schema_encoding "" declares an opaque type (schemaId 0) the projector treats as raw —
//     schema_name / schema_data are meaningful ONLY beside a non-empty schema_encoding.
//
// LIFETIME: the string_views and span ALIAS the caller's bytes; they are copied into the
// stream preamble synchronously inside make_recorder's returned recorder ctor, so the
// referenced bytes must outlive that call (they need not outlive the recorder).
struct type_schema
{
    std::uint64_t              type_id{};
    std::string_view           message_encoding{};
    std::string_view           schema_name{};
    std::string_view           schema_encoding{};
    std::span<const std::byte> schema_data{};
};

}

#endif

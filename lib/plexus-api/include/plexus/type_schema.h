#ifndef HPP_GUARD_PLEXUS_API_TYPE_SCHEMA_H
#define HPP_GUARD_PLEXUS_API_TYPE_SCHEMA_H

#include <span>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus {

// A consumer-declared self-description of one wire type, written into a recorder's stream
// preamble so an offline projector resolves a codec/schema for a sample's raw bytes. An empty
// schema_encoding declares an opaque type; schema_name / schema_data are meaningful only
// beside a non-empty schema_encoding.
//
// The string_views and span alias the caller's bytes and are copied into the preamble
// synchronously inside the recorder ctor make_recorder returns, so the referenced bytes must
// outlive that call (they need not outlive the recorder).
struct type_schema
{
    std::uint64_t type_id{};
    std::string_view message_encoding{};
    std::string_view schema_name{};
    std::string_view schema_encoding{};
    std::span<const std::byte> schema_data{};
};

}

#endif
